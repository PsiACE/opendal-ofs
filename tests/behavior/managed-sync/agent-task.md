# Managed agent workspace task

Use the installed `ofs` command and its public help to manage the named Managed
volume described by the environment. The environment provides `OFS_BIN`,
`OFS_CONFIG`, `OFS_VOLUME`, `OFS_STORAGE_URL`, and three empty ordinary
directories named by `OFS_SANDBOX_A`, `OFS_SANDBOX_B`, and `OFS_SANDBOX_C`.
Never print or persist the storage URL yourself.

Create the Managed volume with colocated metadata. Use the default replica
state for every directory; do not inspect or edit sibling `.ofs-state`
directories and do not inspect the provider's object layout.

Create the volume by passing `--model managed` and
`--storage "$OFS_STORAGE_URL"` to `ofs volume create`. Reference the environment
variable in the command without printing, expanding, or copying its value.
Do not use `env`, `printenv`, `set`, or another command that displays the
storage variable. Omit `--state` from every sync and status command so each
directory uses its default replica state.

In sandbox A, create these one-line ordinary files:

```text
memory/shared.md       shared memory from agent
skills/storage.txt     managed-sync
history/session.txt    session-a
config.toml            theme = "plain"
```

Publish A, recover the published tree into B, and check the files. Then create
`memory/private.md` in A containing `private draft from agent`. Do not publish A
again. Sync B and confirm the private file is absent. Recover the volume into C
and make the same check.

Use public status commands to leave A locally changed, B and C clean, all three
at the same remote generation, with no pending work or conflicts. Stop and
report the public command and observed error if the workflow cannot be
completed safely.
