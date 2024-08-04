use std::{collections::HashMap, slice::SliceIndex};

use polars::{
    chunked_array::builder::NullChunkedBuilder,
    datatypes::{AnyValue, Int64Type},
    frame::{row::AnyValueBuffer, DataFrame},
    prelude::ChunkedBuilder,
};
use serde::{ser::SerializeStruct, Serialize, Serializer};

struct AnyValueBufSerializer<'a> {
    null_queue: usize,
    output: Option<AnyValueBuffer<'a>>,
}
impl<'a> AnyValueBufSerializer<'a> {
    pub fn new() -> Self {
        Self {
            output: None,
            null_queue: 0,
        }
    }
    pub fn finish(&mut self) {
        match &mut self.output {
            _ if self.null_queue == 0 => return,
            Some(buf) => {
                for _ in 0..self.null_queue {
                    buf.add(AnyValue::Null);
                }
            }
            None => {
                self.output = Some(AnyValueBuffer::Null(NullChunkedBuilder::new(
                    "",
                    self.null_queue,
                )))
            }
        }
    }
}

macro_rules! ser_tmeplate {
    ($func: ident, $anyvalbuf: ident, $ty: ty) => {
        fn $func(mut self, v: $ty) -> Result<Self::Ok, Self::Error> {
            if self.output.is_none() {
                self.output = Some(AnyValueBuffer::new(
                    &polars::datatypes::DataType::$anyvalbuf,
                    1,
                ));
            }
            match &mut self.output {
                Some(AnyValueBuffer::$anyvalbuf(b)) => b.append_value(v),
                _ => return Err(()),
            };

            Ok(self)
        }
    };
}

struct AnyValueBufStructSerializer<'a> {
    hashmap: HashMap<String, AnyValueBufSerializer<'a>>,
}
impl<'a> SerializeStruct for AnyValueBufStructSerializer<'a> {
    type Ok = AnyValueBufSerializer<'a>;
    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.hashmap
            .entry(key.to_string())
            .or_insert_with(|| value.serialize(AnyValueBufSerializer::new()));
    }

    type Error;

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl<'a> Serializer for AnyValueBufSerializer<'a> {
    type Ok = AnyValueBufSerializer<'a>;

    type Error;

    type SerializeSeq;

    type SerializeTuple;

    type SerializeTupleStruct;

    type SerializeTupleVariant;

    type SerializeMap;

    type SerializeStruct;

    type SerializeStructVariant;

    ser_tmeplate!(serialize_bool, Boolean, bool);
    ser_tmeplate!(serialize_i8, Int8, i8);
    ser_tmeplate!(serialize_i16, Int16, i16);
    ser_tmeplate!(serialize_i32, Int32, i32);
    ser_tmeplate!(serialize_i64, Int64, i64);

    ser_tmeplate!(serialize_u8, UInt8, u8);
    ser_tmeplate!(serialize_u16, UInt16, u16);
    ser_tmeplate!(serialize_u32, UInt32, u32);
    ser_tmeplate!(serialize_u64, UInt64, u64);

    ser_tmeplate!(serialize_f32, Float32, f32);
    ser_tmeplate!(serialize_f64, Float64, f64);

    ser_tmeplate!(serialize_str, String, &str);

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(v)
    }

    fn serialize_bytes(self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        let output = AnyValueBuffer::All(
            polars::datatypes::DataType::UInt8,
            v.into_iter().map(|i| AnyValue::UInt8(*i)).collect(),
        );
        Ok(self)
    }

    fn serialize_none(mut self) -> Result<Self::Ok, Self::Error> {
        let mutself = &mut self;
        match &mut mutself.output {
            None => mutself.null_queue += 1,
            Some(buf) => {
                buf.add(AnyValue::Null);
            }
        };
        Ok(self)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_unit_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_newtype_struct<T>(
        self,
        name: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_newtype_variant<T>(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        value: &T,
    ) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_seq(self, len: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        todo!()
    }

    fn serialize_tuple(self, len: usize) -> Result<Self::SerializeTuple, Self::Error> {
        todo!()
    }

    fn serialize_tuple_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleStruct, Self::Error> {
        todo!()
    }

    fn serialize_tuple_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeTupleVariant, Self::Error> {
        todo!()
    }

    fn serialize_map(self, len: Option<usize>) -> Result<Self::SerializeMap, Self::Error> {
        todo!()
    }

    fn serialize_struct(
        self,
        name: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStruct, Self::Error> {
        todo!()
    }

    fn serialize_struct_variant(
        self,
        name: &'static str,
        variant_index: u32,
        variant: &'static str,
        len: usize,
    ) -> Result<Self::SerializeStructVariant, Self::Error> {
        todo!()
    }
}
