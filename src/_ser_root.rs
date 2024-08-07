use std::collections::HashMap;

use polars::{
    chunked_array::builder::NullChunkedBuilder,
    frame::row::AnyValueBuffer,
    prelude::{AnyValue, ChunkedBuilder, Field, NamedFrom},
    series::Series,
};
use serde::{
    de::Error,
    ser::{SerializeStruct, SerializeTuple, SerializeTupleStruct},
    Serialize, Serializer,
};

use crate::pl_row_error::PlRowSerdeError;

pub struct SerdeItemAnyValueBuffer<'a> {
    pub anyval_buffer: AnyValueBuffer<'a>,
    pub name: String,
    pub datatype: polars::prelude::DataType,
    pub null_count: usize,
    pub capacity: usize,
}

impl<'a> SerdeItemAnyValueBuffer<'a> {
    fn new(name: &str, capacity: usize) -> Self {
        Self {
            anyval_buffer: AnyValueBuffer::Null(NullChunkedBuilder::new(name, capacity)),
            name: name.to_string(),
            datatype: polars::prelude::DataType::Null,
            null_count: 0,
            capacity,
        }
    }
    pub fn change_not_null_anyvalue_buffer(
        &mut self,
        variant_dt: Option<polars::prelude::DataType>,
    ) {
        let mut data = match (variant_dt, &mut self.anyval_buffer) {
            (None, AnyValueBuffer::Null(_)) => AnyValueBuffer::new(&self.datatype, self.capacity),
            (Some(i), AnyValueBuffer::Null(_)) => AnyValueBuffer::new(&i, self.capacity),
            _ => return,
        };
        for _ in 0..self.null_count {
            data.add(polars::prelude::AnyValue::Null);
        }
        self.null_count = 0; // just in case
        self.anyval_buffer = data;
    }
}

fn hashmap_thing<'a>(hashmap: HashMap<String, SerdeItemAnyValueBuffer<'a>>) -> Vec<Series> {
    let mut stack = vec![];
    for (key, val) in hashmap {
        let s = val.anyval_buffer.into_series().with_name(&key);
        stack.push(s);
    }
    stack
}

fn type_mismatch() -> PlRowSerdeError {
    crate::PlRowSerdeError::custom("type_mismatch")
}

macro_rules! impl_serialize_func {
    ($func: ident, $variant_dt: ident, $arg_dt: ident) => {
        fn $func(mut self, v: $arg_dt) -> Result<Self::Ok, Self::Error> {
            let datatype = polars::prelude::DataType::$variant_dt;
            self.change_not_null_anyvalue_buffer(Some(datatype));
            match &mut self.anyval_buffer {
                AnyValueBuffer::$variant_dt(b) => {
                    b.append_value(v);
                }
                _ => return Err(type_mismatch()),
            };
            Ok(self)
        }
    };
}

impl<'a> Serializer for SerdeItemAnyValueBuffer<'a> {
    type Ok = SerdeItemAnyValueBuffer<'a>;

    type Error = crate::PlRowSerdeError;

    type SerializeSeq = crate::ser_seq::PlRowSerializeSeq<'a>;

    type SerializeTuple = PlRowSerializeTuple<'a>;

    type SerializeTupleStruct = PlRowSerializeTuple<'a>;

    type SerializeTupleVariant;

    type SerializeMap;

    type SerializeStruct;

    type SerializeStructVariant;

    /*
        fn serialize_bool(self, v: bool) -> Result<Self::Ok, Self::Error> {
            match self.inner {
                AnyValueBuffer::Boolean(b) => {
                    b.append_value(v);
                    return Ok(self);
                }
                _ => Err(type_mismatch()),
            }
        }
    */
    impl_serialize_func!(serialize_bool, Boolean, bool);
    impl_serialize_func!(serialize_u8, UInt8, u8);
    impl_serialize_func!(serialize_u16, UInt16, u16);
    impl_serialize_func!(serialize_u32, UInt32, u32);
    impl_serialize_func!(serialize_u64, UInt64, u64);
    impl_serialize_func!(serialize_i8, Int8, i8);
    impl_serialize_func!(serialize_i16, Int16, i16);
    impl_serialize_func!(serialize_i32, Int32, i32);
    impl_serialize_func!(serialize_i64, Int64, i64);
    impl_serialize_func!(serialize_f32, Float32, f32);
    impl_serialize_func!(serialize_f64, Float64, f64);

    //impl_serialize_func!(serialize_char, UInt8, char);
    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        self.change_not_null_anyvalue_buffer(Some(polars::prelude::DataType::String));

        match &mut self.anyval_buffer {
            AnyValueBuffer::String(b) => b.append_value(v.to_string()),
            _ => return Err(type_mismatch()),
        };
        Ok(self)
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        self.change_not_null_anyvalue_buffer(Some(polars::prelude::DataType::String));
        match &mut self.anyval_buffer {
            AnyValueBuffer::String(b) => b.append_value(v),
            _ => return Err(type_mismatch()),
        };
        Ok(self)
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        self.change_not_null_anyvalue_buffer(Some(polars::prelude::DataType::Binary));
        match &mut self.anyval_buffer {
            AnyValueBuffer::All(polars::prelude::DataType::Binary, b) => {
                b.push(polars::prelude::AnyValue::BinaryOwned(v.to_vec()))
            }
            _ => return Err(type_mismatch()),
        };
        Ok(self)
    }

    fn serialize_none(mut self) -> Result<Self::Ok, Self::Error> {
        self.anyval_buffer.add(polars::prelude::AnyValue::Null);
        Ok(self)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(self) -> Result<Self::Ok, Self::Error> {
        self.serialize_none()
    }

    fn serialize_unit_struct(self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        self.serialize_str(name)
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
        value.serialize(self)
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

struct PlRowSerializeStruct<'a> {
    buf: HashMap<&'static str, SerdeItemAnyValueBuffer<'a>>,
    target_capacity: usize,
}

impl<'a> SerializeStruct for PlRowSerializeStruct<'a> {
    type Ok = HashMap<&'static str, SerdeItemAnyValueBuffer<'a>>;

    type Error = crate::PlRowSerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.buf
            .entry(key)
            .or_insert_with(|| SerdeItemAnyValueBuffer::new(key, self.target_capacity));

        let res = value.serialize(self.buf.remove(key).unwrap());

        match res {
            Ok(a) => {
                self.buf.insert(key, a);
                Ok(())
            }
            Err(e) => return Err(e),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.buf)
    }
}

struct PlRowSerializeTuple<'a> {
    buf: HashMap<String, SerdeItemAnyValueBuffer<'a>>,
    target_capacity: usize,
    field_idx: usize,
}
impl<'a> SerializeTuple for PlRowSerializeTuple<'a> {
    type Ok = HashMapThing<'a>;

    type Error = crate::PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        let key = self.field_idx.to_string();
        self.buf
            .entry(key.clone())
            .or_insert_with(|| SerdeItemAnyValueBuffer::new(&key, self.target_capacity));

        let res = value.serialize(self.buf.remove(&key).unwrap());

        match res {
            Ok(a) => {
                self.buf.insert(key, a);
                Ok(())
            }
            Err(e) => return Err(e),
        }
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self.buf)
    }
}

impl<'a> SerializeTupleStruct for PlRowSerializeTuple<'a> {
    type Ok = SerdeItemAnyValueBuffer<'a>;

    type Error = crate::PlRowSerdeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.serialize_element(value)
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        for (key, val) in self.buf {
            val.anyval_buffer;
        }
        Ok(self.buf)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde::Serialize;

    #[test]
    fn serialize_int8() {
        #[derive(Serialize)]
        struct Int8Test {
            int8: i8,
        }
        let it = Int8Test { int8: 0 };
        let int8 = 0i8;
        let mut buf = SerdeItemAnyValueBuffer::new("int8", 1);
        int8.serialize(buf);
    }
}
