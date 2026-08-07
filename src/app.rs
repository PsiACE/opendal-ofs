// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.

use std::collections::BTreeMap;
use std::env;
use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use ofs::catalog::{Catalog, VolumeDefinition};
use ofs::filesystem::{VolumeId, VolumeModel};
use ofs::managed::{
    D1Config, D1Metadata, ManagedError, ManagedErrorKind, ManagedFormat, Metadata, ObjectMetadata,
};
use opendal::Operator;
use url::Url;

use crate::cli::{Cli, Command, VolumeCommand, VolumeCreateArgs};

pub(crate) async fn run(cli: Cli) -> Result<()> {
    match (cli.command, cli.mount_path, cli.backend) {
        (
            Some(Command::Volume {
                command: VolumeCommand::Create(args),
            }),
            None,
            None,
        ) => create_volume(cli.config.as_deref(), args).await,
        (None, Some(mount_path), Some(backend)) => mount(&mount_path, &backend).await,
        (Some(_), _, _) => {
            bail!("a subcommand cannot be combined with Direct Mount arguments; run `ofs --help`")
        }
        (None, _, _) => bail!(
            "provide `volume create ...` or both MOUNT_PATH and BACKEND_URL; run `ofs --help`"
        ),
    }
}

async fn create_volume(config: Option<&Path>, args: VolumeCreateArgs) -> Result<()> {
    if args.model != VolumeModel::Managed {
        bail!(
            "named Direct volumes are not implemented; use `ofs MOUNT_PATH BACKEND_URL` for Direct Mount"
        );
    }

    let mut catalog = match config {
        Some(path) => Catalog::load(path),
        None => Catalog::load_from_env(),
    }
    .context("cannot open the volume catalog; set --config or OFS_CONFIG to a writable path")?;

    let volume_id = catalog
        .get(&args.alias)
        .map(|definition| definition.volume_id)
        .unwrap_or_else(VolumeId::generate);
    let definition = VolumeDefinition::new(
        volume_id,
        args.model,
        args.storage.clone(),
        args.metadata.clone(),
        1,
    )
    .context("volume URLs must be credential-free; supply credentials through provider environment variables")?;
    let created = catalog.create(&args.alias, definition.clone())?;
    let format = ManagedFormat::v1(volume_id, definition.storage.to_string())?;
    let metadata = open_metadata(&args.storage, args.metadata.as_ref())?;
    metadata
        .create_format(&format)
        .await
        .map_err(create_format_error)?;

    catalog
        .save()
        .context("the remote format is ready but the local catalog could not be saved; fix the catalog path and repeat the same command")?;

    let action = if created { "created" } else { "opened" };
    println!("{action} managed volume {:?} with format v1", args.alias);
    Ok(())
}

fn open_metadata(storage: &Url, metadata: Option<&Url>) -> Result<Metadata> {
    let data = open_operator(storage)?;
    match metadata {
        None => Ok(Metadata::Object(ObjectMetadata::new(data))),
        Some(url) if url.scheme() == "d1" => Ok(Metadata::D1(D1Metadata::new(d1_config(url)?))),
        Some(_) => bail!("--metadata must use d1://ACCOUNT/DATABASE/STORE"),
    }
}

fn open_operator(url: &Url) -> Result<Operator> {
    let mut arguments = url.query_pairs().into_owned().collect::<Vec<_>>();
    if url.scheme() == "s3" {
        if let Some(bucket) = url.host_str()
            && !arguments.iter().any(|(key, _)| key == "bucket")
        {
            arguments.push(("bucket".into(), bucket.into()));
        }
        let root = url.path().trim_matches('/');
        if !root.is_empty() && !arguments.iter().any(|(key, _)| key == "root") {
            arguments.push(("root".into(), root.into()));
        }
    }
    Operator::via_iter(url.scheme(), arguments).map_err(|_| {
        anyhow!("cannot configure --storage; check its scheme, endpoint, bucket, and root")
    })
}

fn d1_config(url: &Url) -> Result<D1Config> {
    let account = url
        .host_str()
        .context("--metadata needs a D1 account in its host")?;
    let path = url
        .path_segments()
        .context("--metadata needs /DATABASE/STORE")?
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>();
    if path.len() != 2 {
        bail!("--metadata path must be /DATABASE/STORE");
    }
    let query = url.query_pairs().collect::<BTreeMap<_, _>>();
    if query.contains_key("token") {
        bail!("remove token from --metadata and set OFS_D1_TOKEN instead");
    }
    let token = env::var("OFS_D1_TOKEN")
        .map_err(|_| anyhow!("set OFS_D1_TOKEN to the D1 API credential and repeat the command"))?;
    let mut config = D1Config::new(account, path[0], path[1], token).map_err(|_| {
        anyhow!("invalid D1 metadata configuration; check account, database, store, and token")
    })?;
    if let Some(api_base) = query.get("api_base") {
        config = config
            .with_api_base(api_base.as_ref())
            .map_err(|_| anyhow!("invalid D1 api_base; provide an absolute Query API base URL"))?;
    }
    Ok(config)
}

fn create_format_error(error: ManagedError) -> anyhow::Error {
    match error.kind() {
        ManagedErrorKind::Invalid => anyhow!(
            "cannot create Managed format v1 because the storage lacks required conditional writes or the format binding is invalid"
        ),
        ManagedErrorKind::Conflict => anyhow!(
            "storage is already bound to another Managed volume; use the matching alias and catalog or choose another storage root"
        ),
        ManagedErrorKind::Corrupt => anyhow!(
            "the existing Managed format is corrupt or unsupported; inspect it before attempting recovery"
        ),
        ManagedErrorKind::Unavailable => anyhow!(
            "Managed metadata is unavailable; check the endpoint and credentials, then repeat the same command"
        ),
    }
}

#[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "macos"))]
async fn mount(mount_path: &Path, backend_url: &Url) -> Result<()> {
    use fuse3::MountOptions;
    use fuse3::path::Session;
    use std::env;

    if backend_url.has_host() {
        log::warn!("backend host will be ignored");
    }
    let backend = Operator::via_iter(backend_url.scheme(), backend_url.query_pairs().into_owned())
        .map_err(|_| anyhow!("cannot configure BACKEND_URL; check its scheme and arguments"))?;

    let mut options = MountOptions::default();
    let mut gid: u32 = nix::unistd::getgid().into();
    let mut uid: u32 = nix::unistd::getuid().into();
    if nix::unistd::getuid().is_root() {
        if let Some(sudo_gid) = env::var("SUDO_GID")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            gid = sudo_gid;
        }
        if let Some(sudo_uid) = env::var("SUDO_UID")
            .ok()
            .and_then(|value| value.parse().ok())
        {
            uid = sudo_uid;
        }
    }
    options.gid(gid).uid(uid);
    let filesystem = fuse3_opendal::Filesystem::new(backend, uid, gid);
    let mut handle = if nix::unistd::getuid().is_root() {
        Session::new(options).mount(filesystem, mount_path).await?
    } else {
        Session::new(options)
            .mount_with_unprivileged(filesystem, mount_path)
            .await?
    };
    tokio::select! {
        result = &mut handle => result?,
        _ = tokio::signal::ctrl_c() => handle.unmount().await?,
    }
    Ok(())
}

#[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "macos")))]
async fn mount(_: &Path, _: &Url) -> Result<()> {
    bail!("Direct Mount is supported on Linux, FreeBSD, and macOS")
}
