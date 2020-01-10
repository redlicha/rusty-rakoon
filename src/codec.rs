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

use crate::llio::*;
use crate::protocol::*;

use bytes::BytesMut;
use std;
use tokio_util::codec::{Decoder, Encoder};

/// Arakoon protocol on top of llio.

/// A magic value that is `OR`ed with the opcode. Except when it's not (prologue).
const MAGIC : i32 = 0xb1ff_0000u32 as i32;

#[derive(Clone, Debug)]
struct ActionEncoder;

impl ActionEncoder {
    fn new() -> ActionEncoder {
        ActionEncoder
    }
}

impl Encoder for ActionEncoder {
    type Item = Action;
    type Error = std::io::Error;

    fn encode(&mut self, action: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        let mut i32_encoder = I32Encoder::new();
        let mut data_encoder = DataEncoder::new();
        let mut opt_encoder = OptionEncoder::<DataEncoder>::new(DataEncoder::new());
        match action {
            Action::Set{key, value} => {
                i32_encoder.encode(0x1, buf)?;
                data_encoder.encode(key, buf)?;
                data_encoder.encode(value, buf)
            }
            Action::Delete{key} => {
                i32_encoder.encode(0x2, buf)?;
                data_encoder.encode(key, buf)
            }
            Action::UserFunction{function, arg} => {
                i32_encoder.encode(0x7, buf)?;
                data_encoder.encode(BytesMut::from(function.as_str()), buf)?;
                opt_encoder.encode(arg, buf)
            }
            Action::Assert{key, value} => {
                i32_encoder.encode(0x8, buf)?;
                data_encoder.encode(key, buf)?;
                opt_encoder.encode(value, buf)
            }
            Action::AssertExists{key} => {
                i32_encoder.encode(0xf, buf)?;
                data_encoder.encode(key, buf)
            }
        }
    }
}

/// Codec for the Arakoon Protocol
pub enum Codec {
    SendRequest,
    RecvStatus(Opcode),
    RecvResponse(Box<dyn Decoder<Item=Response, Error=std::io::Error>>)
}

fn write_opcode(opcode: Opcode, buf: &mut BytesMut) -> std::io::Result<()> {
    I32Encoder::new().encode(MAGIC | (opcode as i32), buf)
}

fn write_consistency(consistency: Consistency, buf: &mut BytesMut) -> std::io::Result<()> {
    let mut encoder = I8Encoder::new();
    match consistency {
        Consistency::Consistent => encoder.encode(0x0, buf),
        Consistency::NoGuarantees => encoder.encode(0x1, buf),
        Consistency::AtLeast(stamp) => {
            encoder.encode(0x2, buf)?;
            I64Encoder::new().encode(stamp.0, buf)
        },
    }
}

fn write_prologue(cluster_id: ClusterId, buf: &mut BytesMut) -> std::io::Result<Codec> {
    let mut i32_encoder = I32Encoder::new();
    i32_encoder.encode(MAGIC, buf)?;
    i32_encoder.encode(Opcode::Ping as i32, buf)?;
    DataEncoder::new().encode(BytesMut::from(cluster_id.0.as_str()), buf)?;
    // no response expected
    Ok(Codec::SendRequest)
}

fn write_hello(cluster_id: &ClusterId, node_id: &NodeId, buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(Opcode::Ping, buf)?;
    let mut encoder = DataEncoder::new();
    encoder.encode(BytesMut::from(node_id.0.as_str()), buf)?;
    encoder.encode(BytesMut::from(cluster_id.0.as_str()), buf)?;
    Ok(Codec::RecvStatus(Opcode::Ping))
}

fn write_who_master(buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(Opcode::WhoMaster, buf)?;
    Ok(Codec::RecvStatus(Opcode::WhoMaster))
}

fn write_get_op(opcode: Opcode,
                consistency: Consistency,
                key: BytesMut,
                buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(opcode, buf)?;
    write_consistency(consistency, buf)?;
    DataEncoder::new().encode(key, buf)?;
    Ok(Codec::RecvStatus(opcode))
}

fn write_set(key: BytesMut, val: BytesMut, buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(Opcode::Set, buf)?;
    let mut encoder = DataEncoder::new();
    encoder.encode(key, buf)?;
    encoder.encode(val, buf)?;
    Ok(Codec::RecvStatus(Opcode::Set))
}

fn write_delete_op(opcode: Opcode, key: BytesMut, buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(opcode, buf)?;
    DataEncoder::new().encode(key, buf)?;
    Ok(Codec::RecvStatus(opcode))
}

fn write_range_op(opcode: Opcode,
                  consistency: Consistency,
                  first_key: Option<BytesMut>,
                  include_first: bool,
                  last_key: Option<BytesMut>,
                  include_last: bool,
                  max_entries: i32,
                  buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(opcode, buf)?;
    write_consistency(consistency, buf)?;
    let mut opt_encoder = OptionEncoder::<DataEncoder>::new(DataEncoder::new());
    opt_encoder.encode(first_key, buf)?;
    let mut bool_encoder = BoolEncoder::new();
    bool_encoder.encode(include_first, buf)?;
    opt_encoder.encode(last_key, buf)?;
    bool_encoder.encode(include_last, buf)?;
    I32Encoder::new().encode(max_entries, buf)?;
    Ok(Codec::RecvStatus(opcode))
}

fn write_prefix_keys(consistency: Consistency,
                     prefix: BytesMut,
                     max_entries: i32,
                     buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(Opcode::PrefixKeys, buf)?;
    write_consistency(consistency, buf)?;
    DataEncoder::new().encode(prefix, buf)?;
    I32Encoder::new().encode(max_entries, buf)?;
    Ok(Codec::RecvStatus(Opcode::PrefixKeys))
}

fn write_test_and_set(key: BytesMut,
                      old: Option<BytesMut>,
                      new: Option<BytesMut>,
                      buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(Opcode::TestAndSet, buf)?;
    DataEncoder::new().encode(key, buf)?;
    let mut encoder = OptionEncoder::<DataEncoder>::new(DataEncoder::new());
    encoder.encode(old, buf)?;
    encoder.encode(new, buf)?;
    Ok(Codec::RecvStatus(Opcode::TestAndSet))
}

fn write_sequence_op(opcode: Opcode,
                     actions: Vec<Action>,
                     buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(opcode, buf)?;
    // TODO: avoid copying into the tmp buf
    let mut tmp = BytesMut::new();
    // magic value alert
    I32Encoder::new().encode(0x5, &mut tmp)?;
    VectorEncoder::<ActionEncoder>::new(ActionEncoder::new()).encode(actions, &mut tmp)?;
    DataEncoder::new().encode(tmp, buf)?;
    Ok(Codec::RecvStatus(opcode))
}

fn write_user_function(fun: String,
                       arg: Option<BytesMut>,
                       buf: &mut BytesMut) -> std::io::Result<Codec> {
    write_opcode(Opcode::UserFunction, buf)?;
    DataEncoder::new().encode(BytesMut::from(fun.as_str()), buf)?;
    OptionEncoder::<DataEncoder>::new(DataEncoder::new()).encode(arg, buf)?;
    Ok(Codec::RecvStatus(Opcode::UserFunction))
}

impl Codec {
    pub fn new() -> Codec {
        Codec::SendRequest
    }

}

impl Default for Codec {
    fn default() -> Self {
        Self::new()
    }
}

impl Encoder for Codec {
    type Item = Request;
    type Error = std::io::Error;

    fn encode(&mut self, req: Self::Item, buf: &mut BytesMut) ->
        std::io::Result<()> {
            trace!("req: {:?}", req);
            match *self {
                Codec::SendRequest => (),
                _ => panic!("wrong codec state while encoding"),
            }

            *self = match req {
                Request::Prologue{cluster_id} => write_prologue(cluster_id, buf)?,
                Request::Hello{cluster_id, node_id} => write_hello(&cluster_id,
                                                                   &node_id,
                                                                   buf)?,
                Request::WhoMaster => write_who_master(buf)?,
                Request::Exists{consistency,
                                key} => write_get_op(Opcode::Exists,
                                                     consistency,
                                                     key,
                                                     buf)?,
                Request::Get{consistency, key} => write_get_op(Opcode::Get,
                                                               consistency,
                                                               key,
                                                               buf)?,
                Request::Set{key, value} => write_set(key, value, buf)?,
                Request::Delete{key} => write_delete_op(Opcode::Delete,
                                                        key,
                                                        buf)?,
                Request::Range{consistency,
                               first_key,
                               include_first,
                               last_key,
                               include_last,
                               max_entries} => write_range_op(Opcode::Range,
                                                              consistency,
                                                              first_key,
                                                              include_first,
                                                              last_key,
                                                              include_last,
                                                              max_entries,
                                                              buf)?,
                Request::PrefixKeys{consistency,
                                    prefix,
                                    max_entries} => write_prefix_keys(consistency,
                                                                      prefix,
                                                                      max_entries,
                                                                      buf)?,
                Request::TestAndSet{key, old, new} => write_test_and_set(key,
                                                                         old,
                                                                         new,
                                                                         buf)?,
                Request::RangeEntries{consistency,
                                      first_key,
                                      include_first,
                                      last_key,
                                      include_last,
                                      max_entries} => write_range_op(Opcode::RangeEntries,
                                                                     consistency,
                                                                     first_key,
                                                                     include_first,
                                                                     last_key,
                                                                     include_last,
                                                                     max_entries,
                                                                     buf)?,
                Request::Sequence{actions} => write_sequence_op(Opcode::Sequence,
                                                                actions,
                                                                buf)?,
                Request::UserFunction{function,
                                      arg} => write_user_function(function,
                                                                  arg,
                                                                  buf)?,
                Request::SyncedSequence{actions} => write_sequence_op(Opcode::SyncedSequence,
                                                                      actions,
                                                                      buf)?,
                Request::DeletePrefix{prefix} => write_delete_op(Opcode::DeletePrefix,
                                                                 prefix,
                                                                 buf)?,
            };

            Ok(())
        }
}

// TODO: avoid copy
fn buf_to_string(buf: &BytesMut) -> std::io::Result<String> {
    match String::from_utf8(buf.to_vec()) {
        Ok(s) => Ok(s),
        Err(_) => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                                          "unexpected byte sequence, expected string")),
    }
}

fn make_ok_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    Box::new(ReturnDecoder::<Response>::new(Response::Ok))
}

fn make_string_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        let s = buf_to_string(&val)?;
        Ok(Some(Response::String(s)))
    });
    Box::new(BindDecoder::<Response, DataDecoder>::new(DataDecoder::new(), fun))
}

fn make_node_id_option_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        let res = match val {
            Some(buf) => Some(NodeId(buf_to_string(&buf)?)),
            None => None,
        };

        Ok(Some(Response::NodeIdOption(res)))
    });

    type Decoder = OptionDecoder<DataDecoder>;
    Box::new(BindDecoder::<Response, Decoder>::new(Decoder::new(DataDecoder::new()), fun))
}

fn make_bool_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        Ok(Some(Response::Bool(val)))
    });
    Box::new(BindDecoder::<Response, BoolDecoder>::new(BoolDecoder::new(), fun))
}

fn make_data_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        Ok(Some(Response::Data(val)))
    });
    Box::new(BindDecoder::<Response, DataDecoder>::new(DataDecoder::new(), fun))
}

fn make_data_option_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        Ok(Some(Response::DataOption(val)))
    });

    type Decoder = OptionDecoder<DataDecoder>;
    let decoder = Decoder::new(DataDecoder::new());
    Box::new(BindDecoder::<Response, Decoder>::new(decoder, fun))
}

fn make_data_vec_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        Ok(Some(Response::DataVec(val)))
    });

    type Decoder = VectorDecoder<DataDecoder>;
    let decoder = Decoder::new(DataDecoder::new(), true);
    Box::new(BindDecoder::<Response, Decoder>::new(decoder, fun))

}

fn make_data_pair_vec_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        Ok(Some(Response::DataPairVec(val)))
    });

    type PDecoder = PairDecoder<DataDecoder, DataDecoder>;
    let pdecoder = PDecoder::new(DataDecoder::new(),
                                 DataDecoder::new());

    type Decoder = VectorDecoder<PDecoder>;
    let decoder = Decoder::new(pdecoder, true);
    Box::new(BindDecoder::<Response, Decoder>::new(decoder, fun))
}

fn make_count_rsp_decoder() -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(|val| {
        if val < 0 {
            Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                                    "count < 0!?"))
        } else {
            Ok(Some(Response::Count(val as u32)))
        }
    });

    Box::new(BindDecoder::<Response, I32Decoder>::new(I32Decoder::new(), fun))
}

fn make_ok_status_decoder(opcode: Opcode) -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    match opcode {
        Opcode::Ping => make_string_rsp_decoder(),
        Opcode::WhoMaster => make_node_id_option_rsp_decoder(),
        Opcode::Exists => make_bool_rsp_decoder(),
        Opcode::Get => make_data_rsp_decoder(),
        Opcode::Set | Opcode::Delete | Opcode::Sequence | Opcode::SyncedSequence  => make_ok_rsp_decoder(),
        Opcode::Range | Opcode::PrefixKeys => make_data_vec_rsp_decoder(),
        Opcode::TestAndSet | Opcode::UserFunction => make_data_option_rsp_decoder(),
        Opcode::RangeEntries => make_data_pair_vec_rsp_decoder(),
        Opcode::DeletePrefix => make_count_rsp_decoder(),
    }
}

// TODO: Reconsider handling of ErrorCode::UnknownErrorCode: std::io::Error instead?
fn make_err_status_decoder(status: ErrorCode) -> Box<dyn Decoder<Item=Response, Error=std::io::Error>> {
    let fun = Box::new(move |val| {
        let msg = buf_to_string(&val)?;
        Ok(Some(Response::Error(ErrorResponse::new(status, msg))))
    });

    Box::new(BindDecoder::<Response, DataDecoder>::new(DataDecoder::new(), fun))
}

impl Decoder for Codec {
    type Item = Response;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        if let Codec::SendRequest = *self {
            panic!("wrong codec state while decoding");
        }

        if let Codec::RecvStatus(opcode) = *self {
            *self = match I32Decoder::new().decode(buf)? {
                None => return Ok(None),
                Some(status) if status == 0 =>
                    Codec::RecvResponse(make_ok_status_decoder(opcode)),
                Some(status) =>
                    Codec::RecvResponse(make_err_status_decoder(ErrorCode::from(status))),
            }
        }

        let res = {
            if let Codec::RecvResponse(ref mut decoder) = *self {
                decoder.decode(buf)?
            } else {
                unreachable!()
            }
        };

        if res.is_none() {
            Ok(None)
        } else {
            *self = Codec::SendRequest;
            Ok(res)
        }
    }
}
