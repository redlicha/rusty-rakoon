// Copyright (C) 2018-2020 Arne Redlich <arne.redlich@googlemail.com>
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

//! Types used for the Arakoon RPC protocol.
use bytes::{Bytes,BytesMut};
use num;
use std;
use std::fmt::Display;

/// Arakoon node ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub String);

impl Display for NodeId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Arakoon Cluster ID.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ClusterId(pub String);

impl Display for ClusterId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A stamp used for consistency requirements.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Stamp(pub i64);

/// Consistency requirement.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Consistency {
    Consistent,
    NoGuarantees,
    AtLeast(Stamp),
}

/// The building blocks of sequences.
#[derive(Clone, Debug, PartialEq)]
pub enum Action {
    Set { key: Bytes,
          value: Bytes },
    Delete { key: Bytes },
    Assert { key: Bytes,
             value: Option<Bytes> },
    AssertExists { key: Bytes },
    UserFunction { function: String,
                   arg: Option<Bytes> },
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Opcode {
    Ping = 0x1,
    WhoMaster = 0x2,
    Exists = 0x7,
    Get = 0x8,
    Set = 0x9,
    Delete = 0xa,
    Range = 0xb,
    PrefixKeys = 0xc,
    TestAndSet = 0xd,
    RangeEntries = 0xf,
    Sequence = 0x10,
    UserFunction = 0x15,
    SyncedSequence = 0x24,
    DeletePrefix = 0x27,
    UserHook = 0x45,
}

/// Request sent to the server.
#[derive(Clone, Debug, PartialEq)]
pub enum Request {
    Prologue { cluster_id: ClusterId },
    Hello { cluster_id: ClusterId,
            node_id: NodeId },
    WhoMaster,
    Exists { consistency: Consistency,
             key: Bytes },
    Get { consistency: Consistency,
          key: Bytes },
    Set { key: Bytes,
          value: Bytes },
    Delete { key: Bytes },
    Range { consistency: Consistency,
            first_key: Option<Bytes>,
            include_first: bool,
            last_key: Option<Bytes>,
            include_last: bool,
            max_entries: i32 },
    PrefixKeys { consistency: Consistency,
                 prefix: Bytes,
                 max_entries: i32 },
    TestAndSet { key: Bytes,
                 old: Option<Bytes>,
                 new: Option<Bytes> },
    RangeEntries { consistency: Consistency,
                   first_key: Option<Bytes>,
                   include_first: bool,
                   last_key: Option<Bytes>,
                   include_last: bool,
                   max_entries: i32 },
    Sequence { actions: Vec<Action> },
    UserFunction { function: String,
                   arg: Option<Bytes> },
    SyncedSequence { actions: Vec<Action> },
    DeletePrefix { prefix: Bytes },
    UserHook { consistency: Consistency,
               hook: String },
}

/// Response codes sent back from the server side in case of errors.
///
/// This is actually not quite correct: `UnknownErrorCode` is never sent by the
/// server but rather a catch-all for possibly newly introduced error codes we
/// don't know about.
#[derive(Clone, Copy, Debug, Eq, FromPrimitive, PartialEq)]
pub enum ErrorCode {
    UnknownErrorCode = -1,
    NoMagic = 0x1,
    TooManyDeadNodes = 0x2,
    NoHello = 0x3,
    NotMaster = 0x4,
    NotFound = 0x5,
    WrongCluster = 0x6,
    AssertionFailed = 0x7,
    ReadOnly = 0x8,
    NurseryRangeError = 0x9,
    UnknownFailure = 0xff,
}

impl From<i32> for ErrorCode {
    fn from(val: i32) -> Self {
        match num::FromPrimitive::from_i32(val) {
            Some(error_code) => error_code,
            None => ErrorCode::UnknownErrorCode,
        }
    }
}

/// An error response received from the server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ErrorResponse {
    pub code : ErrorCode,
    pub message : String,
}

impl ErrorResponse {
    pub fn new(code: ErrorCode, message: String) -> ErrorResponse {
        ErrorResponse{code, message}
    }
}

/// Response from the server.
#[derive(Clone, Debug, PartialEq)]
pub enum Response {
    Ok,
    Error(ErrorResponse),
    String(String),
    NodeIdOption(Option<NodeId>),
    Bool(bool),
    Data(BytesMut),
    DataOption(Option<BytesMut>),
    DataVec(Vec<BytesMut>),
    DataPairVec(Vec<(BytesMut, BytesMut)>),
    Count(u32)
}
