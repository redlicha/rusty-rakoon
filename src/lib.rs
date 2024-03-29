// Copyright (C) 2016-2020 Arne Redlich <arne.redlich@googlemail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#[macro_use]
extern crate log;
#[macro_use]
extern crate num_derive;

pub mod client;
pub mod codec;
pub mod llio;
pub mod protocol;

// The official client interface.
pub use crate::client::{ClusterConfig, Error, Node, NodeConfig};
pub use crate::protocol::{Action, ClusterId, Consistency, ErrorCode, NodeId, Stamp};
