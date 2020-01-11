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

//! Arakoon client built on top of tokio.
use crate::codec::*;
use crate::protocol::{
    Action,
    ClusterId,
    Consistency,
    ErrorCode,
    ErrorResponse,
    NodeId,
    Request,
    Response,
};

use bytes::BytesMut;

use futures_util::{
    sink::{
        SinkExt,
    },
};

use std::{
    error,
    fmt::{
        Display,
    },
    net::{
        AddrParseError,
        SocketAddr,
    },
};

use tokio::{
    net::{
        TcpStream,
    },
    stream::{
        StreamExt,
    },
    sync::{
        mpsc,
        oneshot
    },
};

use tokio_util::codec::{
    Decoder,
};

use tower_service::Service;

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

async fn dispatch(cluster_id: ClusterId, tcp_stream: TcpStream, mut rx: mpsc::Receiver<Message>) ->
    std::io::Result<()> {
    let mut framed = Codec::new().framed(tcp_stream);
    framed.send(Request::Prologue{cluster_id}).await?;

    while let Some((req, tx)) = rx.next().await {
        trace!("sending request {:?}", req);
        let res = framed.send(req).await;
        if res.is_err() {
            error!("error sending request: {:?}", res);
            return res;
        }

        trace!("waiting for response");

        let res = {
            if let Some(rsp) = framed.next().await {
                trace!("got response: {:?} - forwarding it via channel", rsp);
                tx.send(rsp)
            } else {
                error!("other side of TCP conn gone");
                tx.send(Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                                "other side of TCP conn gone!?")))
            }
        };

        if let Err(e) = res {
                error!("error forwarding response over channel: {:?}", e);
                return Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                               "other side of channel gone!?"));
        }
    }

    Ok(())
}

pub struct Node {
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    tx: mpsc::Sender<Message>,
    _task_handle: tokio::task::JoinHandle<std::io::Result<()>>,
}

macro_rules! call {
    ($self:ident, $req:expr, $pat:pat => $res:expr) => (
        match $self.call($req).await {
            $pat => $res,
            Ok(Response::Error(ErrorResponse{code, message})) =>
                Err(Error::ErrorResponse(code, message)),
            Ok(_) => Err(Error::ErrorResponse(ErrorCode::UnknownErrorCode,
                                              "server sent unexpected response".to_string())),
            Err(e) => Err(Error::IoError(e)),
        }
    )
}

/// Client for a specific arakoon cluster node, implements the
/// `tower_service::Service` trait.
impl Node {
    pub async fn connect(cluster_id: ClusterId,
                         node_config: &NodeConfig) -> std::io::Result<Node> {
        info!("connecting to {}", node_config.node_id);

        let (tx, rx) = mpsc::channel(1);
        let tcp_stream = TcpStream::connect(node_config.address).await?;
        let cid = cluster_id.clone();
        let task_handle = tokio::task::spawn(async move {
            dispatch(cid, tcp_stream, rx)
                .await
                .map_err(|e| {
                    error!("dispatch returned error: {:?}", e);
                    e
                })
        });

        Ok(Node{cluster_id,
                node_id: node_config.node_id.clone(),
                tx,
                _task_handle: task_handle})
    }

    pub async fn who_master(&mut self) -> std::result::Result<Option<NodeId>, Error> {
        call!(self,
              Request::WhoMaster,
              Ok(Response::NodeIdOption(maybe_node_id)) => Ok(maybe_node_id))
    }

    pub async fn hello(&mut self) -> std::result::Result<String, Error> {
        call!(self,
              Request::Hello{cluster_id: self.cluster_id.clone(),
                             node_id: self.node_id.clone()},
              Ok(Response::String(s)) => Ok(s))
    }

    pub async fn exists(&mut self, consistency: Consistency, key: BytesMut) -> std::result::Result<bool, Error> {
        call!(self,
              Request::Exists{consistency, key},
              Ok(Response::Bool(b)) => Ok(b))
    }

    pub async fn set(&mut self, key: BytesMut, value: BytesMut) -> std::result::Result<(), Error> {
        call!(self,
              Request::Set{key, value},
              Ok(Response::Ok) => Ok(()))
    }

    pub async fn get(&mut self, consistency: Consistency, key: BytesMut) -> std::result::Result<BytesMut, Error> {
        call!(self,
              Request::Get{consistency, key},
              Ok(Response::Data(val)) => Ok(val))
    }

    pub async fn delete(&mut self, key: BytesMut) -> std::result::Result<(), Error> {
        call!(self,
              Request::Delete{key},
              Ok(Response::Ok) => Ok(()))
    }

    pub async fn test_and_set(&mut self, key: BytesMut, old: Option<BytesMut>, new: Option<BytesMut>)
                              -> std::result::Result<Option<BytesMut>, Error> {
        call!(self,
              Request::TestAndSet{key, old, new},
              Ok(Response::DataOption(maybe_buf)) => Ok(maybe_buf))
    }

    pub async fn sequence(&mut self, actions: Vec<Action>) -> std::result::Result<(), Error> {
        call!(self,
              Request::Sequence{actions},
              Ok(Response::Ok) => Ok(()))
    }

    pub async fn synced_sequence(&mut self, actions: Vec<Action>) -> std::result::Result<(), Error> {
        call!(self,
              Request::SyncedSequence{actions},
              Ok(Response::Ok) => Ok(()))
    }

    pub async fn prefix_keys(&mut self, consistency: Consistency, prefix: BytesMut, max_entries: i32)
                             -> std::result::Result<Vec<BytesMut>, Error> {
        call!(self,
              Request::PrefixKeys{consistency, prefix, max_entries},
              Ok(Response::DataVec(vec)) => Ok(vec))
    }

    pub async fn delete_prefix(&mut self, prefix: BytesMut) -> std::result::Result<u32, Error> {
        call!(self,
              Request::DeletePrefix{prefix},
              Ok(Response::Count(n)) => Ok(n))
    }

    pub async fn range(&mut self,
                       consistency: Consistency,
                       first_key: Option<BytesMut>,
                       include_first: bool,
                       last_key: Option<BytesMut>,
                       include_last: bool,
                       max_entries: i32) -> std::result::Result<Vec<BytesMut>, Error> {
        call!(self,
              Request::Range{consistency,
                             first_key,
                             include_first,
                             last_key,
                             include_last,
                             max_entries},
              Ok(Response::DataVec(vec)) => Ok(vec))
    }

    pub async fn range_entries(&mut self,
                               consistency: Consistency,
                               first_key: Option<BytesMut>,
                               include_first: bool,
                               last_key: Option<BytesMut>,
                               include_last: bool,
                               max_entries: i32) -> std::result::Result<Vec<(BytesMut, BytesMut)>, Error> {
        call!(self,
              Request::RangeEntries{consistency,
                                    first_key,
                                    include_first,
                                    last_key,
                                    include_last,
                                    max_entries},
              Ok(Response::DataPairVec(vec)) => Ok(vec))
    }

    pub async fn user_function(&mut self, function: String, arg: Option<BytesMut>)
                               -> std::result::Result<Option<BytesMut>, Error> {
        call!(self,
              Request::UserFunction{function, arg},
              Ok(Response::DataOption(opt)) => Ok(opt))
    }
}

impl Service<Request> for Node {
    type Response = Response;
    type Error = std::io::Error;
    type Future = std::pin::Pin<Box<dyn std::future::Future<Output=Result<Self::Response, Self::Error>>>>;

    fn poll_ready(&mut self, _cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        debug!("poll_ready called");
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request) -> Self::Future {
        let (tx, rx) = oneshot::channel();
        let mut sender = self.tx.clone();

        let fut = async move {
            match sender.send((req, tx)).await {
                Ok(()) => {
                    match rx.await {
                        Ok(rsp) => {
                            rsp
                        },
                        Err(e) => {
                            error!("failed to get response from task: {:?}", e);
                            Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                                    "call: failed to recv - other side gone!?"))
                        },
                    }
                },
                Err(e) => {
                    error!("failed to send request to task: {:?}", e);
                    Err(std::io::Error::new(std::io::ErrorKind::BrokenPipe,
                                            "call: failed to recv - other side gone!?"))

                },
            }
        };

        Box::pin(fut)
    }
}
