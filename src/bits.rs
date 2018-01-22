// Copyright (C) 2016 Arne Redlich <arne.redlich@googlemail.com>
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

extern crate byteorder;

use self::byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};

use std;
use std::error;
use std::io::{Read, Write};
use std::fmt::Debug;
// use std::net::{SocketAddr, ToSocketAddrs};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct NodeId(pub String);

impl std::fmt::Display for NodeId {
    fn fmt(&self, f : &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

// type SockAddr = ToSocketAddrs<Iter=SocketAddr>;

#[derive(Clone, Debug)]
pub struct NodeConfig {
    pub id : NodeId,
    pub addr : String, // Box<SockAddr>,
}

impl NodeConfig {
    pub fn new(id : &NodeId, addr : &str) -> NodeConfig {
        NodeConfig { id : id.clone(),
                     addr : addr.to_owned() }
    }
}

#[derive(Clone, Debug)]
pub struct ClusterId(pub String);

impl std::fmt::Display for ClusterId {
    fn fmt(&self, f : &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Clone, Debug)]
pub struct ClusterConfig {
    pub id : ClusterId,
    pub node_configs : Vec<NodeConfig>,
}

impl ClusterConfig {
    pub fn new(id : &ClusterId, cfgs : Vec<NodeConfig>) -> ClusterConfig {
        ClusterConfig { id : id.clone(),
                        node_configs : cfgs }
    }
}

#[derive(Debug, Eq, PartialEq)]
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

#[derive(Debug)]
pub enum Error {
    ErrorResponse(ErrorCode, String),
    IoError(std::io::Error)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f : &mut std::fmt::Formatter) -> std::fmt::Result {
        match *self {
            Error::ErrorResponse(ref code, ref msg) => write!(f, "Arakoon error response {:?} : {}", code, msg),
            Error::IoError(ref err) => write!(f, "I/O error: {}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::ErrorResponse(_, _) => "Arakoon error response",
            Error::IoError(ref err) => err.description(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(e : std::io::Error) -> Error {
        Error::IoError(e)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Action<'k, 'v> {
    Set { key : &'k [u8], value : &'v [u8] },
    Delete { key : &'k [u8] },
    Assert { key : &'k [u8], value : Option<&'v [u8]> },
    AssertExists { key : &'k [u8] },
    UserFunction { fun : &'k str, arg : Option<&'v [u8]> },
}

#[derive(Debug)]
pub struct Connection<T : Debug + Read + Write> {
    pub node_id : NodeId,
    pub cluster_id : ClusterId,
    sock: T,
}

#[derive(Clone, Debug)]
pub struct Stamp(i64);

#[derive(Clone, Debug)]
pub enum Consistency {
    Consistent,
    NoGuarantees,
    AtLeast(Stamp),
}

pub type KeysAndVals = Vec<(Vec<u8>, Vec<u8>)>;

const MAGIC : i32 = 0xb1ff0000u32 as i32;

#[derive(Clone, Debug)]
enum Request {
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
    Version = 0x28,
}

// ugly - is there a smarter way? a macro?
fn to_error_code(e : i32) -> ErrorCode {
    assert!(e != 0);
    if e == ErrorCode::NoMagic as i32 {
        ErrorCode::NoMagic
    } else if e == ErrorCode::TooManyDeadNodes as i32 {
        ErrorCode::TooManyDeadNodes
    } else if e == ErrorCode::NoHello as i32 {
        ErrorCode::NoHello
    } else if e == ErrorCode::NotMaster as i32 {
        ErrorCode::NotMaster
    } else if e == ErrorCode::NotFound as i32 {
        ErrorCode::NotFound
    } else if e == ErrorCode::WrongCluster as i32 {
        ErrorCode::WrongCluster
    } else if e == ErrorCode::AssertionFailed as i32 {
        ErrorCode::AssertionFailed
    } else if e == ErrorCode::ReadOnly as i32 {
        ErrorCode::ReadOnly
    } else if e == ErrorCode::NurseryRangeError as i32 {
        ErrorCode::NurseryRangeError
    } else if e == ErrorCode::UnknownFailure as i32 {
        ErrorCode::UnknownFailure
    } else {
        ErrorCode::UnknownErrorCode
    }
}

fn send_i32<W: Write>(w : &mut W, num : i32) -> std::io::Result<()> {
    w.write_i32::<LittleEndian>(num)?;
    Ok(())
}

fn send_i64<W: Write>(w : &mut W, num : i64) -> std::io::Result<()> {
    w.write_i64::<LittleEndian>(num)?;
    Ok(())
}

fn recv_i64<R: Read>(r : &mut R) -> Result<i64> {
    match r.read_i64::<LittleEndian>() {
        Err(e) => Err(Error::IoError(e)),
        Ok(n) => Ok(n),
    }
}

fn send_consistency<W: Write>(w : &mut W, c : &Consistency) -> std::io::Result<()> {
    match c {
        &Consistency::Consistent => send_byte(w, 0x0 as u8)?,
        &Consistency::NoGuarantees => send_byte(w, 0x1 as u8)?,
        &Consistency::AtLeast(ref stamp) => {
            send_byte(w, 0x2 as u8)?;
            send_i64(w, stamp.0)?
        }
    }

    Ok(())
}

fn send_req<W: Write>(w : &mut W, cmd : Request) -> std::io::Result<()> {
    send_i32(w, MAGIC | cmd as i32)
}

fn send_byte<W: Write>(w : &mut W, byte : u8) -> std::io::Result<()> {
    w.write_u8(byte)?;
    Ok(())
}

fn send_bool<W: Write>(w : &mut W, b : bool) -> std::io::Result<()> {
    if b {
        send_byte(w, 1 as u8)
    } else {
        send_byte(w, 0 as u8)
    }
}

fn send_option<W: Write>(w : &mut W, opt : Option<&[u8]>) -> std::io::Result<()> {
    match opt {
        Some(ref buf) => {
            send_bool(w, true)?;
            send_buf(w, buf)?;
        }
        None => {
            send_bool(w, false)?;
        }
    }

    Ok(())
}

fn send_buf<W: Write>(w : &mut W, buf : &[u8]) -> std::io::Result<()> {
    send_i32(w, buf.len() as i32)?;
    w.write_all(&buf)?;
    Ok(())
}

fn send_action<W: Write>(w: &mut W, act : &Action) -> std::io::Result<()> {
    match act {
        &Action::Set{key, value} => {
            send_i32(w, 1)?;
            send_buf(w, key)?;
            send_buf(w, value)
        },
        &Action::Delete{key} => {
            send_i32(w, 2)?;
            send_buf(w, key)
        },
        &Action::UserFunction{fun, arg} => {
            send_i32(w, 7)?;
            send_buf(w, fun.as_bytes())?;
            send_option(w, arg)
        },
        &Action::Assert{key, value} => {
            send_i32(w, 8)?;
            send_buf(w, key)?;
            send_option(w, value)
        }
        &Action::AssertExists{key} => {
            send_i32(w, 15)?;
            send_buf(w, key)
        },
    }
}

fn send_sequence<W: Write>(w: &mut W, acts : &[Action]) -> std::io::Result<()> {
    let mut cur = std::io::Cursor::new(Vec::new());
    send_i32(&mut cur, 5 as i32)?;
    send_i32(&mut cur, acts.len() as i32)?;
    for ref a in acts {
        send_action(&mut cur, a)?;
    }

    send_buf(w, &cur.into_inner())
}

fn recv_i32<R: Read>(r : &mut R) -> Result<i32> {
    match r.read_i32::<LittleEndian>() {
        Err(e) => Err(Error::IoError(e)),
        Ok(n) => Ok(n),
    }
}

fn recv_bool<R: Read>(r : &mut R) -> Result<bool> {
    let mut byte = [ 0u8 ];
    let res = r.read_exact(&mut byte);
    if let Err(e) = res {
        return Err(Error::IoError(e));
    }

    match byte[0] {
        0 => Ok(false),
        _ => Ok(true),
    }
}

fn recv_option<R: Read>(r : &mut R) -> Result<Option<Vec<u8>>> {
    match recv_bool(r) {
        Err(e) => Err(e),
        Ok(false) => Ok(None),
        Ok(true) => match recv_buf(r) {
            Err(e) => Err(e),
            Ok(buf) => Ok(Some(buf)),
        }
    }
}

fn recv_buf<R: Read>(r : &mut R) -> Result<Vec<u8>> {
    match recv_i32(r) {
        Err(e) => Err(e),
        Ok(len) => {
            let mut buf = vec![0u8; len as usize];
            match r.read_exact(&mut buf) {
                Ok(_) => Ok(buf),
                Err(e) => Err(Error::IoError(e)),
            }
        }
    }
}

fn recv_rsp<R: Read>(r : &mut R) -> Result<()> {
    match recv_i32(r) {
        Ok(0) => Ok(()),
        Ok(e) => {
            let msg = recv_buf(r);
            match msg {
                Ok(buf) => Err(Error::ErrorResponse(to_error_code(e),
                                                    String::from_utf8(buf).unwrap())),
                Err(_) => Err(Error::ErrorResponse(to_error_code(e),
                                                   "(malformed error reply without msg!)".to_owned())),
            }
        },
        Err(e) => Err(e)
    }
}

impl<T> Connection<T> where T : Debug + Read + Write {
    pub fn new(sock : T, cluster_id : &ClusterId, node_id : &NodeId) -> Connection<T> {
        Connection::<T> { node_id : node_id.clone(),
                          cluster_id : cluster_id.clone(),
                          sock : sock }
    }

    pub fn prologue(&mut self) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_i32(sock, MAGIC)?;
        send_i32(sock, Request::Ping as i32)?;
        let cid = self.cluster_id.0.clone();
        send_buf(sock, &cid.as_bytes())?;
        Ok(())
    }

    pub fn version_req(&mut self) -> std::io::Result<()> {
        send_req(&mut self.sock, Request::Version)
    }

    pub fn who_master_req(&mut self) -> std::io::Result<()> {
        send_req(&mut self.sock, Request::WhoMaster)
    }

    pub fn who_master_rsp(&mut self) -> Result<Option<NodeId>> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        let maybe_master = recv_option(sock)?;
        Ok(maybe_master.map(|buf| { NodeId(String::from_utf8(buf).unwrap()) }))
    }

    pub fn hello_req(&mut self, id : &str) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::Ping)?;
        send_buf(sock, &id.as_bytes())?;
        let cid = self.cluster_id.0.clone();
        send_buf(sock, &cid.as_bytes())
    }

    pub fn hello_rsp(&mut self) -> Result<Vec<u8>> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        recv_buf(sock)
    }

    pub fn exists_req(&mut self, consistency : &Consistency, key : &[u8]) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::Exists)?;
        send_consistency(sock, consistency)?;
        send_buf(sock, key)
    }

    pub fn exists_rsp(&mut self) -> Result<bool> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        recv_bool(sock)
    }

    pub fn get_req(&mut self, consistency : &Consistency, key : &[u8]) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::Get)?;
        send_consistency(sock, consistency)?;
        send_buf(sock, key)
    }

    pub fn get_rsp(&mut self) -> Result<Vec<u8>> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        recv_buf(sock)
    }

    pub fn set_req(&mut self, key : &[u8], val : &[u8]) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::Set)?;
        send_buf(sock, key)?;
        send_buf(sock, val)
    }

    pub fn set_rsp(&mut self) -> Result<()> {
        recv_rsp(&mut self.sock)
    }

    pub fn delete_req(&mut self, key : &[u8]) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::Delete)?;
        send_buf(sock, key)
    }

    pub fn delete_rsp(&mut self) -> Result<()> {
        recv_rsp(&mut self.sock)
    }

    fn send_range_req(&mut self,
                      req : Request,
                      consistency : &Consistency,
                      first_key : Option<&[u8]>,
                      include_first : bool,
                      last_key : Option<&[u8]>,
                      include_last : bool,
                      max_entries : i32) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, req)?;
        send_consistency(sock, consistency)?;
        send_option(sock, first_key)?;
        send_bool(sock, include_first)?;
        send_option(sock, last_key)?;
        send_bool(sock, include_last)?;
        send_i32(sock, max_entries)
    }

    pub fn range_req(&mut self,
                     consistency : &Consistency,
                     first_key : Option<&[u8]>,
                     include_first : bool,
                     last_key : Option<&[u8]>,
                     include_last : bool,
                     max_entries : i32) -> std::io::Result<()> {
        self.send_range_req(Request::Range,
                            consistency,
                            first_key,
                            include_first,
                            last_key,
                            include_last,
                            max_entries)
    }

    fn recv_keys_rsp(&mut self) -> Result<Vec<Vec<u8>>> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        let n = recv_i32(sock)?;
        let mut v = Vec::new();
        v.reserve(n as usize);
        for _ in 0..n {
            v.push(recv_buf(sock)?);
        }

        v.reverse();
        Ok(v)
    }

    pub fn range_rsp(&mut self) -> Result<Vec<Vec<u8>>> {
        self.recv_keys_rsp()
    }

    pub fn range_entries_req(&mut self,
                             consistency : &Consistency,
                             first_key : Option<&[u8]>,
                             include_first : bool,
                             last_key : Option<&[u8]>,
                             include_last : bool,
                             max_entries : i32) -> std::io::Result<()> {
        self.send_range_req(Request::RangeEntries,
                            consistency,
                            first_key,
                            include_first,
                            last_key,
                            include_last,
                            max_entries)
    }

    pub fn range_entries_rsp(&mut self) -> Result<KeysAndVals> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        let n = recv_i32(sock)?;
        let mut vec = Vec::new();
        vec.reserve(n as usize);
        for _ in 0..n {
            let k = recv_buf(sock)?;
            let v = recv_buf(sock)?;
            vec.push((k, v));
        }

        vec.reverse();
        Ok(vec)
    }

    pub fn prefix_keys_req(&mut self,
                           consistency : &Consistency,
                           prefix : &[u8],
                           max_entries : i32) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::PrefixKeys)?;
        send_consistency(sock, consistency)?;
        send_buf(sock, prefix)?;
        send_i32(sock, max_entries)
    }

    pub fn prefix_keys_rsp(&mut self) -> Result<Vec<Vec<u8>>> {
        self.recv_keys_rsp()
    }

    pub fn test_and_set_req(&mut self,
                            key : &[u8],
                            old : Option<&[u8]>,
                            new : Option<&[u8]>) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::TestAndSet)?;
        send_buf(sock, key)?;
        send_option(sock, old)?;
        send_option(sock, new)
    }

    fn recv_option_rsp(&mut self) -> Result<Option<Vec<u8>>> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        recv_option(sock)
    }

    pub fn test_and_set_rsp(&mut self) -> Result<Option<Vec<u8>>> {
        self.recv_option_rsp()
    }

    pub fn delete_prefix_req(&mut self, pfx : &[u8]) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::DeletePrefix)?;
        send_buf(sock, pfx)
    }

    pub fn delete_prefix_rsp(&mut self) -> Result<i32> {
        let ref mut sock = self.sock;
        recv_rsp(sock)?;
        recv_i32(sock)
    }

    pub fn user_function_req(&mut self, fun : &str, arg : Option<&[u8]>) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, Request::UserFunction)?;
        send_buf(sock, fun.as_bytes())?;
        send_option(sock, arg)
    }

    pub fn user_function_rsp(&mut self) -> Result<Option<Vec<u8>>> {
        self.recv_option_rsp()
    }

    fn send_seq_req(&mut self,
                    req : Request,
                    acts : &[Action]) -> std::io::Result<()> {
        let ref mut sock = self.sock;
        send_req(sock, req)?;
        send_sequence(sock, acts)
    }

    pub fn sequence_req(&mut self,
                        acts : &[Action]) -> std::io::Result<()> {
        self.send_seq_req(Request::Sequence, acts)
    }

    pub fn sequence_rsp(&mut self) -> Result<()> {
        recv_rsp(&mut self.sock)
    }

    pub fn synced_sequence_req(&mut self,
                               acts : &[Action]) -> std::io::Result<()> {
        self.send_seq_req(Request::SyncedSequence, acts)
    }

    pub fn synced_sequence_rsp(&mut self) -> Result<()> {
        recv_rsp(&mut self.sock)
    }
}

#[cfg(test)]
mod test {

    use super::*;
    use super::{MAGIC, Request, recv_i32, send_req, to_error_code};
    use std::io::Cursor;

    #[test]
    fn sequence() {
        let key = "key".to_owned();
        let val = "val".to_owned();
        let del = "old_key".to_owned();
        let fun = "user_function".to_owned();

        let seq = vec![Action::Assert{key : key.as_bytes(), value : None },
                       Action::Set{key : key.as_bytes(), value : val.as_bytes() },
                       Action::Delete{key : del.as_bytes() },
                       Action::UserFunction{ fun : &fun, arg : None }];

        assert_eq!(4, seq.len());
    }

    #[test]
    fn node_id() {
        println!("Node Id: {:?}", NodeId("RustyRakoon".to_owned()))
    }

    #[test]
    fn error_codes() {
        assert_eq!(ErrorCode::UnknownErrorCode,
                   to_error_code(0xbadbeef));
        assert_eq!(ErrorCode::NoMagic,
                   to_error_code(ErrorCode::NoMagic as i32));
        assert_eq!(ErrorCode::TooManyDeadNodes,
                   to_error_code(ErrorCode::TooManyDeadNodes as i32));
        assert_eq!(ErrorCode::NoHello,
                   to_error_code(ErrorCode::NoHello as i32));
        assert_eq!(ErrorCode::NotMaster,
                   to_error_code(ErrorCode::NotMaster as i32));
        assert_eq!(ErrorCode::NotFound,
                   to_error_code(ErrorCode::NotFound as i32));
        assert_eq!(ErrorCode::WrongCluster,
                   to_error_code(ErrorCode::WrongCluster as i32));
        assert_eq!(ErrorCode::AssertionFailed,
                   to_error_code(ErrorCode::AssertionFailed as i32));
        assert_eq!(ErrorCode::ReadOnly,
                   to_error_code(ErrorCode::ReadOnly as i32));
        assert_eq!(ErrorCode::NurseryRangeError,
                   to_error_code(ErrorCode::NurseryRangeError as i32));
        assert_eq!(ErrorCode::UnknownFailure,
                   to_error_code(ErrorCode::UnknownFailure as i32));
    }

    #[test]
    #[should_panic]
    fn zero_aint_no_error_code() {
        to_error_code(0);
    }

    #[test]
    fn requests() {
        let check = |req : Request| -> () {
            let mut w = vec![];
            assert!(send_req(&mut w, req.clone()).is_ok());

            let mut r = Cursor::new(w);
            let rsp = recv_i32(& mut r);
            assert!(rsp.is_ok());
            assert_eq!(rsp.unwrap(),
                       MAGIC | req as i32);
        };

        check(Request::Ping);
        check(Request::WhoMaster);
        check(Request::Exists);
        check(Request::Get);
        check(Request::Set);
        check(Request::Delete);
        check(Request::Range);
        check(Request::RangeEntries);
        check(Request::Sequence);
        check(Request::PrefixKeys);
        check(Request::TestAndSet);
        check(Request::DeletePrefix);
        check(Request::UserFunction);
        check(Request::SyncedSequence);
        check(Request::Version);
    }
}
