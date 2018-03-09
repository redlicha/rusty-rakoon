// Copyright (C) 2018 Arne Redlich <arne.redlich@googlemail.com>
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

//! Arakoon client built on top of tokio.
use bytes::BytesMut;

use codec::*;

use futures::{Sink, Stream};
use futures::future::{err, Executor, Future, ok};
use futures::stream::{SplitSink, SplitStream};
use futures::sync::{oneshot, mpsc};
use futures::sync::mpsc::Receiver;

use protocol::{Action, ClusterId, Consistency, ErrorCode, ErrorResponse, NodeId, Request, Response};

use std;
use std::error;
use std::fmt::Display;
use std::net::{AddrParseError, SocketAddr};
use std::rc::Rc;

use tokio::net::TcpStream;
use tokio_io::AsyncRead;
use tokio_io::codec::Framed;
use tokio_service::Service;

#[derive(Debug)]
pub enum Error {
    ErrorResponse(ErrorCode, String),
    IoError(std::io::Error)
}

impl Display for Error {
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

/// Config of an Arakoon cluster member.
#[derive(Clone, Debug, PartialEq)]
pub struct NodeConfig {
    pub node_id : NodeId,
    pub address : SocketAddr,
}

impl NodeConfig {
    pub fn new(node_id: NodeId, addr: &str) -> std::result::Result<NodeConfig, AddrParseError> {
        let address = addr.parse()?;
        Ok(NodeConfig{node_id,
                      address})
    }
}

/// Config of an Arakoon cluster.
#[derive(Clone, Debug, PartialEq)]
pub struct ClusterConfig {
    pub cluster_id : ClusterId,
    pub node_configs : Vec<NodeConfig>,
}

impl ClusterConfig {
    pub fn new(cluster_id: ClusterId, node_configs: Vec<NodeConfig>) -> ClusterConfig {
        ClusterConfig{cluster_id,
                      node_configs}
    }
}

// An mpsc channel is used for sending (requests,oneshot channel) pairs to the
// task spawned on the executor. The oneshot channel is used to communicate the
// response back to the client.
type Message = (Request, oneshot::Sender<Result<Response, std::io::Error>>);
type NodeSink = SplitSink<Framed<TcpStream, Codec>>;
type NodeStream = SplitStream<Framed<TcpStream, Codec>>;

fn dispatch(sink: NodeSink, stream: NodeStream, rx: Receiver<Message>) -> Box<Future<Item=(), Error=()>> {
    let fut = rx.fold((sink, stream), move |(sink, stream), (req, tx)| {
        trace!("sending request {:?}", req);
        sink.send(req)
            .map_err(|e| {
                error!("error sending request: {:?}", e)
            }).map(move |sink| {
                stream.into_future()
                    .map_err(|(e, _)| {
                        error!("into future error: {:?}", e)
                    })
                    .map(move |(maybe_rsp, stream)| {
                        trace!("got response {:?}", maybe_rsp);
                        let f = match maybe_rsp {
                            Some(rsp) => tx.send(Ok(rsp)),
                            None => tx.send(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                                                    "other side of channel gone!?"))),
                        };

                        f.map_err(|e| {
                            error!("error returning response: {:?}", e)
                        }).map(move |_| {
                            Ok((sink, stream))
                        })
                    })
            }).flatten().flatten().flatten()
    }).map_err(|e| {
        // TODO: bubble the error up to the caller
        error!("error dispatching requests: {:?}", e)
    }).map(|_| {
        debug!("done!");
    });

    Box::new(fut)
}

pub struct Node {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    tx: mpsc::Sender<Message>,
}

/// Client for a specific arakoon cluster node, implements the
/// `tokio_service::Service` trait.
///
/// TODO: pass the `Executor` directly by reference without `Rc` (it's there to
/// shut up the borrow checker wrt the lifetime of the executor in
/// `connect_to_master`.
impl Node {
    pub fn connect<E>(cluster_id: ClusterId,
                      node_config: &NodeConfig,
                      executor: Rc<E>) -> std::io::Result<Node>
    where
        E : Executor<Box<Future<Item=(), Error=()>>>,
    {
        let (tx, rx) = mpsc::channel(1);
        let node_id = node_config.node_id.clone();
        let node = Node{cluster_id: cluster_id.clone(),
                        node_id,
                        tx};
        info!("connecting to to {}", node_config.node_id);
        let task = TcpStream::connect(&node_config.address)
            .map_err(|e| {
                error!("error connecting: {:?}", e)
            })
            .map(move |tcp_stream| {
                let (sink, stream) = tcp_stream.framed(Codec::new()).split();
                sink.send(Request::Prologue{cluster_id})
                    .map_err(|e| {
                        error!("error sending prologue: {:?}", e)
                    })
                    .map(move |sink| {
                        dispatch(sink, stream, rx)
                    })
            }).flatten().flatten();

        match executor.execute(Box::new(task)) {
            Ok(()) => Ok(node),
            Err(e) => Err(std::io::Error::new(std::io::ErrorKind::Other,
                                              format!("executor returned error: {:?}", e))),
        }
    }

    // These all follow the same pattern - use a macro.
    pub fn who_master(&self) -> Box<Future<Item=Option<NodeId>, Error=Error>> {
        Box::new(self.call(Request::WhoMaster)
                 .then(|ret| {
                     match ret {
                         Ok(Response::NodeIdOption(maybe_node_id)) => Ok(maybe_node_id),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn hello(&self) -> Box<Future<Item=String, Error=Error>> {
        Box::new(self.call(Request::Hello{cluster_id: self.cluster_id.clone(),
                                          node_id: self.node_id.clone()})
                 .then(|ret| {
                     match ret {
                         Ok(Response::String(s)) => Ok(s),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn exists(&self, consistency: Consistency, key: BytesMut) -> Box<Future<Item=bool, Error=Error>> {
        Box::new(self.call(Request::Exists{consistency, key})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Bool(b)) => Ok(b),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn set(&self, key: BytesMut, value: BytesMut) -> Box<Future<Item=(), Error=Error>> {
        Box::new(self.call(Request::Set{key, value})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Ok) => Ok(()),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn get(&self, consistency: Consistency, key: BytesMut) -> Box<Future<Item=BytesMut, Error=Error>> {
        Box::new(self.call(Request::Get{consistency, key})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Data(val)) => Ok(val),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn delete(&self, key: BytesMut) -> Box<Future<Item=(), Error=Error>> {
        Box::new(self.call(Request::Delete{key})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Ok) => Ok(()),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn test_and_set(&self, key: BytesMut, old: Option<BytesMut>, new: Option<BytesMut>)
                        -> Box<Future<Item=Option<BytesMut>, Error=Error>> {
        Box::new(self.call(Request::TestAndSet{key, old, new})
                 .then(|ret| {
                     match ret {
                         Ok(Response::DataOption(maybe_buf)) => Ok(maybe_buf),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn sequence(&self, actions: Vec<Action>) -> Box<Future<Item=(), Error=Error>> {
        Box::new(self.call(Request::Sequence{actions})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Ok) => Ok(()),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn synced_sequence(&self, actions: Vec<Action>) -> Box<Future<Item=(), Error=Error>> {
        Box::new(self.call(Request::SyncedSequence{actions})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Ok) => Ok(()),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn prefix_keys(&self, consistency: Consistency, prefix: BytesMut, max_entries: i32)
                       -> Box<Future<Item=Vec<BytesMut>, Error=Error>> {
        Box::new(self.call(Request::PrefixKeys{consistency, prefix, max_entries})
                 .then(|ret| {
                     match ret {
                         Ok(Response::DataVec(vec)) => Ok(vec),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn delete_prefix(&self, prefix: BytesMut) -> Box<Future<Item=u32, Error=Error>> {
        Box::new(self.call(Request::DeletePrefix{prefix})
                 .then(|ret| {
                     match ret {
                         Ok(Response::Count(n)) => Ok(n),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn range(&self,
                 consistency: Consistency,
                 first_key: Option<BytesMut>,
                 include_first: bool,
                 last_key: Option<BytesMut>,
                 include_last: bool,
                 max_entries: i32) -> Box<Future<Item=Vec<BytesMut>, Error=Error>> {
        Box::new(self.call(Request::Range{consistency,
                                          first_key,
                                          include_first,
                                          last_key,
                                          include_last,
                                          max_entries})
                 .then(|ret| {
                     match ret {
                         Ok(Response::DataVec(vec)) => Ok(vec),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn range_entries(&self,
                         consistency: Consistency,
                         first_key: Option<BytesMut>,
                         include_first: bool,
                         last_key: Option<BytesMut>,
                         include_last: bool,
                         max_entries: i32) -> Box<Future<Item=Vec<(BytesMut, BytesMut)>, Error=Error>> {
        Box::new(self.call(Request::RangeEntries{consistency,
                                                 first_key,
                                                 include_first,
                                                 last_key,
                                                 include_last,
                                                 max_entries})
                 .then(|ret| {
                     match ret {
                         Ok(Response::DataPairVec(vec)) => Ok(vec),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

    pub fn user_function(&self, function: String, arg: Option<BytesMut>)
                         -> Box<Future<Item=Option<BytesMut>, Error=Error>> {
        Box::new(self.call(Request::UserFunction{function, arg})
                 .then(|ret| {
                     match ret {
                         Ok(Response::DataOption(opt)) => Ok(opt),
                         Ok(Response::Error(ErrorResponse{code, message})) =>
                             Err(Error::ErrorResponse(code, message)),
                         Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                                           "server sent unexpected response".to_string())),
                         Err(e) => Err(Error::IoError(e)),
                     }
                 }))
    }

}

impl Service for Node {
    type Request = Request;
    type Response = Response;
    type Error = std::io::Error;
    type Future = Box<Future<Item=Self::Response, Error=Self::Error>>;

    fn call(&self, req: Self::Request) -> Self::Future {
        let (tx, rx) = oneshot::channel();
        let fut = self.tx.clone()
            .send((req, tx))
            .map_err(|e| {
                error!("failed to put request on channel: {}", e);
                err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                        "call: failed to send - other side gone!?"))
            })
            .map(move |_| {
                rx.map_err(|e| {
                    error!("call: error receiving from channel: {}", e);
                    err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                            "call: failed to recv - other side gone!?"))
                })
            })
            .flatten()
            .then(|ret| {
                match ret {
                    Ok(Ok(rsp)) => ok(rsp),
                    Ok(Err(e)) => err(e),
                    Err(e) => e,
                }
            });

        Box::new(fut)
    }
}
