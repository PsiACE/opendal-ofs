// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Compact positional encodings for fixed Managed wire records.

macro_rules! tuple_wire {
    ($name:ty { $($field:ident : $field_type:ty),+ $(,)? }) => {
        impl serde::Serialize for $name {
            fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
            where
                S: serde::Serializer,
            {
                serde::Serialize::serialize(
                    &($( &self.$field, )+),
                    serializer,
                )
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
            where
                D: serde::Deserializer<'de>,
            {
                let ($( $field, )+): ($( $field_type, )+) =
                    serde::Deserialize::deserialize(deserializer)?;
                Ok(Self { $( $field, )+ })
            }
        }
    };
}

pub(crate) use tuple_wire;
