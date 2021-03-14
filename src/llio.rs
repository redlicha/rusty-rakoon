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

//! Arakoon (and Alba) Low Level I/O (llio) building blocks.
//!
//! `tokio_io::codec` `Encoder` and `Decoder` for basic types (`i8`, `i32`,
//! `i64`, `Bool`), sequences of bytes and combinators on top of these for
//! `Option`, `Vec` and pair types.
//!
//! TODO:
//! * factor out common code of the I{8,32,64}Codecs, e.g.
//!   trait BasicCodec<T> {
//!      fn load(...);
//!      fn save(...);
//!   }
//!
//!   (or use an associated type instead of a type param?).
//! * move to a crate of its own?
//! * use a distinct error type (that is convertible to `std::io::Error`)?
//! * consider integrating this with `serde`?
use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use std;
use std::io::Cursor;

#[derive(Clone, Copy, Debug, Default)]
pub struct I8Encoder;

/// Encoder for i8 values.
impl I8Encoder {
    pub fn new() -> I8Encoder {
        I8Encoder
    }
}

impl Encoder for I8Encoder {
    type Item = i8;
    type Error = std::io::Error;

    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        let s = std::mem::size_of::<Self::Item>();
        if buf.remaining_mut() < s {
            buf.reserve(s)
        }

        buf.put_i8(val);
        Ok(())
    }
}

/// Decoder for i8 values.
#[derive(Clone, Copy, Debug, Default)]
pub struct I8Decoder;

impl I8Decoder {
    pub fn new() -> I8Decoder {
        I8Decoder
    }
}

impl Decoder for I8Decoder {
    type Item = i8;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        let s = std::mem::size_of::<Self::Item>();
        if buf.len() < s {
            buf.reserve(s);
            Ok(None)
        } else {
            let res = Cursor::new(&mut *buf).get_i8();
            let _ = buf.split_to(s);
            Ok(Some(res))
        }
    }
}

/// Encoder for i32 values.
#[derive(Clone, Copy, Debug, Default)]
pub struct I32Encoder;

impl I32Encoder {
    pub fn new() -> I32Encoder {
        I32Encoder
    }
}

impl Encoder for I32Encoder {
    type Item = i32;
    type Error = std::io::Error;

    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        let s = std::mem::size_of::<Self::Item>();
        if buf.remaining_mut() < s {
            buf.reserve(s)
        }

        buf.put_i32_le(val);
        Ok(())
    }
}

/// Decoder for i32 values.
#[derive(Clone, Copy, Debug, Default)]
pub struct I32Decoder;

impl I32Decoder {
    pub fn new() -> I32Decoder {
        I32Decoder
    }
}

impl Decoder for I32Decoder {
    type Item = i32;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        let s = std::mem::size_of::<Self::Item>();
        if buf.len() < s {
            buf.reserve(s);
            Ok(None)
        } else {
            let res = Cursor::new(&mut *buf).get_i32_le();
            let _= buf.split_to(s);
            Ok(Some(res))
        }
    }
}

/// Encoder for i64 values.
#[derive(Clone, Copy, Debug, Default)]
pub struct I64Encoder;

impl I64Encoder {
    pub fn new() -> I64Encoder {
        I64Encoder
    }
}

impl Encoder for I64Encoder {
    type Item = i64;
    type Error = std::io::Error;

    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        let s = std::mem::size_of::<Self::Item>();
        if buf.remaining_mut() < s {
            buf.reserve(s)
        }

        buf.put_i64_le(val);
        Ok(())
    }
}

/// Decoder for i64 values.
#[derive(Clone, Copy, Debug, Default)]
pub struct I64Decoder;

impl I64Decoder {
    pub fn new() -> I64Decoder {
        I64Decoder
    }
}

impl Decoder for I64Decoder {
    type Item = i64;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        let s = std::mem::size_of::<Self::Item>();
        if buf.len() < s {
            buf.reserve(s);
            Ok(None)
        } else {
            let res = Cursor::new(&mut *buf).get_i64_le();
            let _ = buf.split_to(s);
            Ok(Some(res))
        }
    }
}

/// Fun with monads: inject a value.
pub struct ReturnDecoder<T>
where T: Clone {
    item: T
}

impl<T> ReturnDecoder<T>
where T: Clone {
    pub fn new(item: T) -> ReturnDecoder<T> {
        ReturnDecoder{item}
    }
}

impl<T> Decoder for ReturnDecoder<T>
where T: Clone {
    type Item = T;
    type Error = std::io::Error;

    fn decode(&mut self, _: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        Ok(Some(self.item.clone()))
    }
}

/// Fun with monads: bind.
pub struct BindDecoder<T, D>
where D: Decoder<Error=std::io::Error> + Send {
    decoder: D,
    fun : Box<dyn Fn(D::Item) -> std::io::Result<Option<T>> + Send>,
}

impl<T, D> BindDecoder<T, D>
where D: Decoder<Error=std::io::Error> + Send {
    pub fn new(decoder: D, fun: Box<dyn Fn(D::Item) -> std::io::Result<Option<T>> + Send>) -> BindDecoder<T, D> {
        BindDecoder{decoder, fun}
    }
}

impl<T, D> Decoder for BindDecoder<T, D>
where D: Decoder<Error=std::io::Error> + Send {
    type Item = T;
    type Error = D::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        match self.decoder.decode(buf)? {
            Some(item) => (self.fun)(item),
            None => Ok(None),
        }
    }
}

/// Encoder for Bool values.
#[derive(Clone, Copy, Debug, Default)]
pub struct BoolEncoder;

impl BoolEncoder {
    pub fn new() -> BoolEncoder {
        BoolEncoder
    }
}

impl Encoder for BoolEncoder {
    type Item = bool;
    type Error = std::io::Error;

    fn encode(&mut self, req: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        I8Encoder.encode( if req { 0x1i8 } else { 0x0i8 }, buf)
    }
}

/// Decoder for Bool values.
///
/// We could build it on top of a `BindDecoder`:
///
/// ```
///     extern crate bytes;
///     extern crate rusty_rakoon;
///     extern crate tokio_util;
///
///     type I8Decoder = rusty_rakoon::llio::I8Decoder;
///     type BindDecoder = rusty_rakoon::llio::BindDecoder<bool, I8Decoder>;
///
///     pub struct BoolDecoder(BindDecoder);
///
///     impl BoolDecoder {
///         fn new() -> BoolDecoder {
///             let fun = Box::new(|val: i8|{
///                 match val {
///                     0x0 => Ok(Some(false)),
///                     0x1 => Ok(Some(true)),
///                     _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
///                                                  "unexpected byte sequence, expected bool")),
///                 }
///             });
///
///             BoolDecoder(BindDecoder::new(I8Decoder::new(),
///                                          fun))
///         }
///     }
///
///     impl tokio_util::codec::Decoder for BoolDecoder {
///         type Item = bool;
///         type Error = std::io::Error;
///
///         fn decode(&mut self, buf: &mut bytes::BytesMut) -> std::io::Result<Option<Self::Item>> {
///              self.0.decode(buf)
///         }
///     }
/// ```
/// But that's not much of an improvement over the below implementation and also
/// incurs an extra allocation.
#[derive(Clone, Copy, Debug, Default)]
pub struct BoolDecoder;

impl BoolDecoder {
    pub fn new() -> BoolDecoder {
        BoolDecoder
    }
}

impl Decoder for BoolDecoder {
    type Item = bool;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        match I8Decoder.decode(buf)? {
            Some(0x0) => Ok(Some(false)),
            Some(0x1) => Ok(Some(true)),
            Some(_) => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                                               "unexpected byte sequence, expected bool")),
            None => Ok(None),
        }
    }
}

/// Encoder for strings of bytes.
///
/// TODO: make generic, e.g. using `IntoBuf` (but that incurs another copy?). For
/// the moment it's not yet generic as `DataDecoder` should mirror the type (or
/// is that just a misguided idea of symmetry?) which is tricky as it might e.g.
/// be possible to use `IntoBuf` for `String` but `FromBuf` is not yet available
/// `From<BytesMut>` does not allow a conversion to `String`.
/// Alternatively: use `Vec<u8>` instead of `BytesMut`?
#[derive(Clone, Copy, Debug, Default)]
pub struct DataEncoder;

impl DataEncoder {
    pub fn new() -> DataEncoder {
        DataEncoder
    }
}

impl Encoder for DataEncoder {
    type Item = BytesMut;
    type Error = std::io::Error;

    // TODO: Avoid extra copy due tp BytesMut::from() followed by buf.put()!?
    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        let l = val.len();
        let s = l as i32;
        assert!(s >= 0);
        I32Encoder.encode(s, buf)?;
        buf.extend(val);
        Ok(())
    }
}

/// Decoder for strings of bytes.
#[derive(Clone, Copy, Debug)]
pub enum DataDecoder {
    Length,
    Value(usize),
}

impl DataDecoder {
    pub fn new() -> DataDecoder {
        DataDecoder::Length
    }

    fn decode_length(buf: &mut BytesMut) -> std::io::Result<Option<usize>> {
        match I32Decoder.decode(buf)? {
            Some(len) if len >= 0 => Ok(Some(len as usize)),
            Some(len) => {
                assert!(len < 0);
                Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                                        "data buffer length < 0!?"))
            },
            None => Ok(None)
        }
    }

    fn decode_value(buf: &mut BytesMut, len: usize) -> std::io::Result<Option<BytesMut>> {
        if buf.len() < len {
            Ok(None)
        } else {
            Ok(Some(buf.split_to(len)))
        }
    }
}

impl Default for DataDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl Decoder for DataDecoder {
    type Item = BytesMut;
    type Error = std::io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        let len = match *self {
            DataDecoder::Length => {
                match DataDecoder::decode_length(buf)? {
                    Some(n) => {
                        *self = DataDecoder::Value(n);
                        if buf.len() < n {
                            buf.reserve(n);
                        }
                        n
                    }
                    None => return Ok(None),
                }
            }
            DataDecoder::Value(n) => n
        };

        match DataDecoder::decode_value(buf, len)? {
            Some(data) => {
                *self = DataDecoder::Length;
                Ok(Some(data))
            }
            None => Ok(None)
        }
    }
}

/// Encoder combinator for option types.
#[derive(Clone, Copy, Debug)]
pub struct OptionEncoder<T>
where T: Encoder {
    encoder: T,
}

impl<T> OptionEncoder<T>
where T: Encoder {
    pub fn new(encoder: T) -> OptionEncoder<T> {
        OptionEncoder{encoder}
    }
}

impl<T> Encoder for OptionEncoder<T>
where T: Encoder<Error = std::io::Error> {
    type Item = Option<T::Item>;
    type Error = T::Error;

    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        if let Some(v) = val {
            BoolEncoder.encode(true, buf)?;
            self.encoder.encode(v, buf)
        } else {
            BoolEncoder.encode(false, buf)
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum OptionDecoderState {
    Bool,
    Value,
}

/// Decoder combinator for option types.
#[derive(Clone, Copy, Debug)]
pub struct OptionDecoder<T>
where T: Decoder {
    decoder: T,
    state: OptionDecoderState,
}

impl<T> OptionDecoder<T>
where T: Decoder {
    pub fn new(decoder: T) -> OptionDecoder<T> {
        OptionDecoder{decoder,
                      state: OptionDecoderState::Bool}
    }
}

impl<T> Decoder for OptionDecoder<T>
where T: Decoder<Error = std::io::Error> {
    type Item = Option<T::Item>;
    type Error = T::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        if self.state == OptionDecoderState::Bool {
            match BoolDecoder::new().decode(buf)? {
                None => return Ok(None),
                Some(false) => return Ok(Some(None)),
                Some(true) => self.state = OptionDecoderState::Value,
            }
        }

        assert!(self.state == OptionDecoderState::Value);

        match self.decoder.decode(buf)? {
            None => Ok(None),
            Some(data) => {
                self.state = OptionDecoderState::Bool;
                Ok(Some(Some(data)))
            }
        }
    }
}

/// Encoder combinator for vector types.
#[derive(Clone, Copy, Debug)]
pub struct VectorEncoder<T>
where T: Encoder {
    encoder: T,
}

impl<T> VectorEncoder<T>
where T: Encoder {
    pub fn new(encoder: T) -> VectorEncoder<T> {
        VectorEncoder{encoder}
    }
}

impl<T> Encoder for VectorEncoder<T>
where T: Encoder<Error = std::io::Error> {
    type Item = Vec<T::Item>;
    type Error = T::Error;

    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        I32Encoder.encode(val.len() as i32, buf)?;
        for v in val {
            self.encoder.encode(v, buf)?;
        }

        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
enum VectorDecoderState {
    Length,
    Values(usize),
}

/// Decoder combinator for vector types.
#[derive(Clone, Debug)]
pub struct VectorDecoder<T>
where T: Decoder {
    decoder: T,
    reverse: bool,
    state: VectorDecoderState,
    vec: Vec<T::Item>,
}

impl<T> VectorDecoder<T>
where T: Decoder {
    pub fn new(decoder : T, reverse: bool) -> VectorDecoder<T> {
        VectorDecoder{decoder,
                      reverse,
                      state: VectorDecoderState::Length,
                      vec: Vec::new()}
    }
}

impl<T> Decoder for VectorDecoder<T>
where T: Decoder<Error = std::io::Error> {
    type Item = Vec<T::Item>;
    type Error = T::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        if self.state == VectorDecoderState::Length {
            match I32Decoder.decode(buf)? {
                None => return Ok(None),
                Some(len) if len < 0 => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput,
                                                                       "vector length < 0!?")),
                Some(len) => self.state = VectorDecoderState::Values(len as usize),
            }
        }

        assert!(self.state != VectorDecoderState::Length);

        loop {
            match self.state {
                VectorDecoderState::Length => unreachable!(),
                VectorDecoderState::Values(left) if left == 0 => {
                    self.state = VectorDecoderState::Length;
                    if self.reverse {
                        self.vec.reverse();
                    }
                    return Ok(Some(self.vec.split_off(0)));
                },
                VectorDecoderState::Values(left) => {
                    match self.decoder.decode(buf)? {
                        None => return Ok(None),
                        Some(item) => {
                            self.vec.push(item);
                            self.state = VectorDecoderState::Values(left - 1);
                        }
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PairEncoder<F, S>
where F: Encoder, S: Encoder {
    first_encoder: F,
    second_encoder: S,
}

impl<F, S> PairEncoder<F, S>
where F: Encoder, S: Encoder {
    pub fn new(first_encoder: F, second_encoder: S) -> PairEncoder<F, S> {
        PairEncoder{first_encoder,
                    second_encoder}
    }
}

impl<F, S> Encoder for PairEncoder<F, S>
where F: Encoder<Error = std::io::Error>, S: Encoder<Error = std::io::Error> {
    type Item = (F::Item, S::Item);
    type Error = F::Error;

    fn encode(&mut self, val: Self::Item, buf: &mut BytesMut) -> std::io::Result<()> {
        let (fst, snd) = val;
        self.first_encoder.encode(fst, buf)?;
        self.second_encoder.encode(snd, buf)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum PairDecoderState<F>
where F: PartialEq {
    First,
    // use Option::take to move the value out
    Second(Option<F>)
}

#[derive(Debug, Clone)]
pub struct PairDecoder<F, S>
where F: Decoder, F::Item: PartialEq, S: Decoder {
    first_decoder: F,
    second_decoder: S,
    state: PairDecoderState<F::Item>,
}

impl <F, S> PairDecoder<F, S>
where F: Decoder, F::Item: PartialEq, S: Decoder {
    pub fn new(first_decoder: F, second_decoder: S) -> PairDecoder<F, S> {
        PairDecoder{first_decoder,
                    second_decoder,
                    state: PairDecoderState::First,
        }
    }
}

impl<F, S> Decoder for PairDecoder<F, S>
where F: Decoder<Error = std::io::Error>, F::Item: PartialEq, S: Decoder<Error = std::io::Error> {
    type Item = (F::Item, S::Item);
    type Error = S::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> std::io::Result<Option<Self::Item>> {
        if self.state == PairDecoderState::First {
            match self.first_decoder.decode(buf)? {
                None => return Ok(None),
                Some(fst) => self.state = PairDecoderState::Second(Some(fst)),
            }
        }

        assert!(self.state != PairDecoderState::First);
        let res = {
            if let PairDecoderState::Second(ref mut fst) = self.state {
                assert!(fst.is_some());
                match self.second_decoder.decode(buf)? {
                    None => return Ok(None),
                    Some(snd) => Ok(Some((fst.take().unwrap(),
                                          snd)))
                }
            } else {
                unreachable!();
            }
        };
        // The borrow checker wins for the moment - this would be nicer in the
        // Some() branch of the above match.
        self.state = PairDecoderState::First;
        res
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fmt::Debug;

    fn test_happy_path<E, D, V>(encoder: &mut E, decoder: &mut D, fst: V, snd: V)
    where E: Encoder<Item=V, Error=std::io::Error>,
          D: Decoder<Item=V, Error=std::io::Error>,
          V: Clone + Debug + PartialEq {
        let mut buf = BytesMut::new();
        assert!(buf.is_empty());
        assert!(encoder.encode(fst.clone(), &mut buf).is_ok());
        assert_eq!(fst, decoder.decode(&mut buf).unwrap().unwrap());
        assert!(buf.is_empty());
        assert!(encoder.encode(snd.clone(), &mut buf).is_ok());
        assert_eq!(snd, decoder.decode(&mut buf).unwrap().unwrap());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_i8_happy_path() {
        test_happy_path(&mut I8Encoder::new(), &mut I8Decoder::new(), 1, 2);
    }

    #[test]
    fn test_i32_roundtrip() {
        test_happy_path(&mut I32Encoder::new(),
                        &mut I32Decoder::new(),
                        (1i32 << 31) + 2,
                        (2i32 << 31) + 3);
    }

    #[test]
    fn test_i64_happy_path() {
        test_happy_path(&mut I64Encoder::new(),
                        &mut I64Decoder::new(),
                        (1i64 << 63) + 2,
                        (2i64 << 63) + 3);
    }

    #[test]
    fn test_bool_happy_path() {
        test_happy_path(&mut BoolEncoder::new(), &mut BoolDecoder::new(), true, false);
    }

    #[test]
    fn test_bool_error() {
        let mut buf = BytesMut::new();
        assert!(buf.is_empty());
        assert!(I8Encoder::new().encode(std::i8::MAX, &mut buf).is_ok());
        assert!(!buf.is_empty());
        assert!(BoolDecoder::new().decode(&mut buf).is_err());
    }

    #[test]
    fn test_optional_i32_happy_path() {
        test_happy_path(&mut OptionEncoder::new(I32Encoder::new()),
                        &mut OptionDecoder::new(I32Decoder::new()),
                        None,
                        Some(1i32));
    }

    #[test]
    fn test_pair_happy_path() {
        test_happy_path(&mut PairEncoder::new(I8Encoder::new(), BoolEncoder::new()),
                        &mut PairDecoder::new(I8Decoder::new(), BoolDecoder::new()),
                        (23i8, false),
                        (42i8, true));
    }

    #[test]
    fn test_vector_happy_path() {
        test_happy_path(&mut VectorEncoder::new(I32Encoder::new()),
                        &mut VectorDecoder::new(I32Decoder::new(), false),
                        vec![1i32, 2i32, 3i32],
                        vec![4i32, 5i32, 6i32]);
    }

    #[test]
    fn test_vector_error() {
        let mut buf = BytesMut::new();
        assert!(buf.is_empty());
        assert!(I32Encoder::new().encode(-1i32, &mut buf).is_ok());
        assert!(VectorDecoder::new(I32Decoder::new(), false).decode(&mut buf).is_err());
    }

    #[test]
    fn test_vector_empty() {
        test_happy_path(&mut VectorEncoder::new(I32Encoder::new()),
                        &mut VectorDecoder::new(I32Decoder::new(), false),
                        vec![],
                        vec![]);
    }

    #[test]
    fn test_data_happy_path() {
        let mut fst = BytesMut::with_capacity(32);
        fst.put(&b"first"[..]);

        let mut snd = BytesMut::with_capacity(32);
        snd.put(&b"second"[..]);

        test_happy_path(&mut DataEncoder::new(),
                        &mut DataDecoder::new(),
                        fst,
                        snd);
    }

    #[test]
    fn test_data_error() {
        let mut buf = BytesMut::new();
        assert!(buf.is_empty());
        assert!(I32Encoder::new().encode(-1i32, &mut buf).is_ok());
        assert!(DataDecoder::new().decode(&mut buf).is_err());
    }

    #[test]
    fn test_optional_data_happy_path() {
        let mut fst = BytesMut::with_capacity(32);
        fst.put(&b"first"[..]);

        test_happy_path(&mut OptionEncoder::new(DataEncoder::new()),
                        &mut OptionDecoder::new(DataDecoder::new()),
                        Some(fst),
                        None);
    }

    #[test]
    fn test_return_decoder() {
        let mut buf = BytesMut::new();
        let msg = "injected value".to_owned();
        let mut decoder = ReturnDecoder::<String>::new(msg.clone());
        assert!(buf.is_empty());
        assert_eq!(msg,
                   decoder.decode(&mut buf).unwrap().unwrap());
        assert!(buf.is_empty());
    }

    #[test]
    fn test_bind_decoder() {
        let mut buf = BytesMut::new();
        let n = 21;
        let fun = |val| { Ok(Some(val as i32 * 2)) };
        let expected = fun(n);
        type Decoder = BindDecoder<i32, ReturnDecoder<i16>>;
        let mut decoder = Decoder::new(ReturnDecoder::new(n),
                                       Box::new(fun));
        assert!(buf.is_empty());
        assert_eq!(expected.unwrap(),
                   decoder.decode(&mut buf).unwrap());
        assert!(buf.is_empty());
    }
}
