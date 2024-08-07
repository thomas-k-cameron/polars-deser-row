use std::collections::HashMap;

use polars::{
    frame::DataFrame,
    prelude::{AnyValue, DataType, NamedFrom},
    series::Series,
};
use serde::{
    de::Error,
    ser::{
        SerializeMap, SerializeSeq, SerializeStruct, SerializeStructVariant, SerializeTuple,
        SerializeTupleStruct, SerializeTupleVariant,
    },
    Serialize, Serializer,
};

use crate::pl_row_error::PlRowSerdeError;

pub fn serialize_into_dataframe<T: Serialize>(
    iter: impl Iterator<Item = T>,
) -> Result<DataFrame, serde::de::value::Error> {
    let mut plr = PlRowSerStruct::default();

    for i in iter {
        let res = i.serialize(plr);
        match res {
            Ok(a) => {
                plr = a;
            }
            Err(e) => return Err(e),
        }
    }
    Ok(plr.into_dataframe())
}

#[test]
fn test_serialize_into_series_with_options_primitive() {
    #[derive(serde::Serialize, Default)]
    struct TestStruct {
        option_boolean: Option<bool>,
        option_int8: Option<i8>,
        option_int16: Option<i16>,
        option_int32: Option<i32>,
        option_int64: Option<i64>,
        option_uint8: Option<u8>,
        option_uint16: Option<u16>,
        option_uint32: Option<u32>,
        option_uint64: Option<u64>,
        option_utf8_heap: Option<String>,
        option_utf8_static: Option<&'static str>,
    }
    let t = TestStruct::default();
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_boolean.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_int8.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_int16.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_int32.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_int64.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_uint8.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_uint16.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_uint32.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_uint64.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_utf8_heap.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new_null("", 1)),
        t.option_utf8_static.serialize(PlRowSer::default())
    );

    let t = TestStruct {
        option_boolean: Some(false),
        option_int8: Some(Default::default()),
        option_int16: Some(Default::default()),
        option_int32: Some(Default::default()),
        option_int64: Some(Default::default()),
        option_uint8: Some(Default::default()),
        option_uint16: Some(Default::default()),
        option_uint32: Some(Default::default()),
        option_uint64: Some(Default::default()),
        option_utf8_heap: Some(Default::default()),
        option_utf8_static: Some(Default::default()),
    };
    assert_eq!(
        Ok(Series::new("", [false])),
        t.option_boolean.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i8])),
        t.option_int8.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i16])),
        t.option_int16.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i32])),
        t.option_int32.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i64])),
        t.option_int64.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u8])),
        t.option_uint8.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u16])),
        t.option_uint16.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u32])),
        t.option_uint32.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u64])),
        t.option_uint64.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [""])),
        t.option_utf8_heap.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [""])),
        t.option_utf8_static.serialize(PlRowSer::default())
    );
}

#[test]
fn test_serialize_into_series() {
    #[derive(serde::Serialize, Default)]
    struct TestStruct {
        boolean: bool,
        int8: i8,
        int16: i16,
        int32: i32,
        int64: i64,
        uint8: u8,
        uint16: u16,
        uint32: u32,
        uint64: u64,
        utf8_heap: String,
        utf8_static: &'static str,
        char: char,
    }

    let t = TestStruct::default();
    assert_eq!(
        Ok(Series::new("", [false])),
        t.boolean.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i8])),
        t.int8.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i16])),
        t.int16.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i32])),
        t.int32.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_i64])),
        t.int64.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u8])),
        t.uint8.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u16])),
        t.uint16.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u32])),
        t.uint32.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [0_u64])),
        t.uint64.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [""])),
        t.utf8_heap.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [""])),
        t.utf8_static.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", [(0 as char).to_string()])),
        t.char.serialize(PlRowSer::default())
    );
}

#[test]
fn enum_variant_serialize_into_series() {
    #[derive(Serialize)]
    enum TestEnum {
        Lol,
        Wow,
        Asdf,
    }

    assert_eq!(
        Ok(Series::new("", ["Lol"])),
        TestEnum::Lol.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", ["Wow"])),
        TestEnum::Wow.serialize(PlRowSer::default())
    );
    assert_eq!(
        Ok(Series::new("", ["Asdf"])),
        TestEnum::Asdf.serialize(PlRowSer::default())
    );
}

#[test]
fn test_serialize_into_series_asdf() {
    #[derive(serde::Serialize, Default)]
    struct TestStruct {
        boolean: bool,
        int8: i8,
        int16: i16,
        int32: i32,
        int64: i64,
        uint8: u8,
        uint16: u16,
        uint32: u32,
        uint64: u64,
        utf8_heap: String,
        utf8_static: &'static str,
        char: char,
    }

    let t = TestStruct::default();
    let df = t
        .serialize(PlRowSerStruct::default())
        .unwrap()
        .into_dataframe();

    assert_eq!(df.height(), 1);
    assert_eq!(df.get_columns().len(), 12);

    // bool
    assert_eq!(df["boolean"].get(0).unwrap(), AnyValue::Boolean(false));
    // int
    assert_eq!(df["int8"].get(0).unwrap(), AnyValue::Int8(0));
    assert_eq!(df["int16"].get(0).unwrap(), AnyValue::Int16(0));
    assert_eq!(df["int32"].get(0).unwrap(), AnyValue::Int32(0));
    assert_eq!(df["int64"].get(0).unwrap(), AnyValue::Int64(0));
    // uint
    assert_eq!(df["uint8"].get(0).unwrap(), AnyValue::UInt8(0));
    assert_eq!(df["uint16"].get(0).unwrap(), AnyValue::UInt16(0));
    assert_eq!(df["uint32"].get(0).unwrap(), AnyValue::UInt32(0));
    assert_eq!(df["uint64"].get(0).unwrap(), AnyValue::UInt64(0));
    // letters and stuff
    assert_eq!(
        df["char"].get(0).unwrap(),
        AnyValue::String(&'\0'.to_string())
    );

    assert_eq!(df["utf8_heap"].get(0).unwrap(), AnyValue::String(""));
    assert_eq!(df["utf8_static"].get(0).unwrap(), AnyValue::String(""));
}

#[derive(Default, Debug)]
struct PlRowSer {
    pl_ser: Series,
}

#[derive(Default)]
struct PlSerPlaceHolder;

#[derive(Default, Debug)]
struct PlRowSerSeq {
    pl_ser: Option<PlRowSer>,
}
impl SerializeSeq for PlRowSerSeq {
    type Ok = Series;

    type Error = crate::PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let pl_ser = value.serialize(PlRowSer::default())?;
        match self.pl_ser.as_mut() {
            Some(i) => {
                i.pl_ser.extend(&pl_ser).unwrap();
            }
            None => {
                self.pl_ser = Some(PlRowSer { pl_ser });
            }
        };
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeMap for PlSerPlaceHolder {
    type Ok = Series;
    type Error = crate::PlRowSerdeError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl SerializeTuple for PlSerPlaceHolder {
    type Ok = Series;

    type Error = PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeTupleStruct for PlSerPlaceHolder {
    type Ok = Series;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeTupleVariant for PlSerPlaceHolder {
    type Ok = Series;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeStruct for PlSerPlaceHolder {
    type Ok = Series;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeStructVariant for PlSerPlaceHolder {
    type Ok = Series;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

macro_rules! impl_serialize_func {
    ($func: ident, $variant_dt: ident, $arg_dt: ident) => {
        fn $func(mut self, v: $arg_dt) -> Result<Self::Ok, Self::Error> {
            if self.pl_ser.dtype() == &DataType::Null {
                self.pl_ser = self.pl_ser.cast(&DataType::$variant_dt).unwrap();
            }
            self.pl_ser
                .extend(&Series::new(self.pl_ser.name(), [v]))
                .unwrap();
            Ok(self.pl_ser)
        }
    };
}
impl<'a> Serializer for PlRowSer {
    type Ok = Series;

    type Error = crate::pl_row_error::PlRowSerdeError;

    type SerializeSeq = PlRowSerSeq;

    type SerializeTuple = PlSerPlaceHolder;

    type SerializeTupleStruct = PlSerPlaceHolder;

    type SerializeTupleVariant = PlSerPlaceHolder;

    type SerializeMap = PlSerPlaceHolder;

    type SerializeStruct = PlSerPlaceHolder;

    type SerializeStructVariant = PlSerPlaceHolder;
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

    fn serialize_char(mut self, v: char) -> Result<Self::Ok, Self::Error> {
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = self.pl_ser.cast(&DataType::String).unwrap();
        }
        self.pl_ser
            .append(&Series::new(self.pl_ser.name(), [v.to_string()]))
            .unwrap();
        Ok(self.pl_ser)
    }

    fn serialize_str(mut self, v: &str) -> Result<Self::Ok, Self::Error> {
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = self.pl_ser.cast(&DataType::String).unwrap();
        }
        self.pl_ser
            .append(&Series::new(self.pl_ser.name(), [v]))
            .unwrap();
        Ok(self.pl_ser)
    }

    fn serialize_bytes(mut self, v: &[u8]) -> Result<Self::Ok, Self::Error> {
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = Series::new_null(self.pl_ser.name(), self.pl_ser.len() + 1);
        }
        let mut seq = self.serialize_seq(Some(v.len())).unwrap();
        for i in v {
            seq.serialize_element(i)?;
        }
        seq.end()
    }

    fn serialize_none(mut self) -> Result<Self::Ok, Self::Error> {
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = Series::new_null(self.pl_ser.name(), self.pl_ser.len() + 1);
        }
        Ok(self.pl_ser)
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        value.serialize(self)
    }

    fn serialize_unit(mut self) -> Result<Self::Ok, Self::Error> {
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = self.pl_ser.cast(&DataType::Boolean).unwrap();
        }
        self.pl_ser
            .extend(&Series::new(self.pl_ser.name(), [true]))
            .unwrap();
        Ok(self.pl_ser)
    }

    fn serialize_unit_struct(mut self, name: &'static str) -> Result<Self::Ok, Self::Error> {
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = self.pl_ser.cast(&DataType::String).unwrap();
        }
        self.pl_ser
            .extend_constant(AnyValue::String(name), 1)
            .map_err(|e| PlRowSerdeError::custom(e.to_string()))
    }

    fn serialize_unit_variant(
        mut self,
        _: &'static str,
        _: u32,
        variant: &'static str,
    ) -> Result<Self::Ok, Self::Error> {
        let cat = &DataType::Categorical(None, Default::default());
        if self.pl_ser.dtype() == &DataType::Null {
            self.pl_ser = self.pl_ser.cast(cat).unwrap();
        }
        self.pl_ser
            .extend(
                &Series::new(self.pl_ser.name(), [variant])
                    .cast(cat)
                    .unwrap(),
            )
            .unwrap();
        Ok(self.pl_ser)
    }

    fn serialize_newtype_struct<T>(
        self,
        _name: &'static str,
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
        // tried with struct but it kinda didn't work
        todo!()
    }

    fn serialize_seq(self, _: Option<usize>) -> Result<Self::SerializeSeq, Self::Error> {
        let seq = PlRowSerSeq::default();

        Ok(seq)
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

#[derive(Default, Debug)]
pub struct PlRowSerStruct {
    pl_ser_map: HashMap<&'static str, PlRowSer>,
}

impl PlRowSerStruct {
    pub fn into_dataframe(self) -> DataFrame {
        let columns: Vec<_> = self
            .pl_ser_map
            .into_iter()
            .map(|(_, val)| val.pl_ser)
            .collect();
        DataFrame::new(columns).unwrap()
    }
}
pub struct PlSerPlaceHolder2;
macro_rules! impl_as_todo {
    ($($func: ident, $arg: ty;)*) => {
        $(
            fn $func(self, _: $arg) -> Result<Self::Ok, Self::Error> {
                todo!()
            }
        ) *

    };
}

impl SerializeMap for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;
    type Error = crate::PlRowSerdeError;

    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeTuple for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;

    type Error = PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeTupleStruct for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeTupleVariant for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeStruct for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeStructVariant for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;

    type Error = PlRowSerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}
impl SerializeSeq for PlSerPlaceHolder2 {
    type Ok = PlRowSerStruct;

    type Error = crate::pl_row_error::PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }
}

impl<'a> Serializer for PlRowSerStruct {
    type Ok = PlRowSerStruct;

    type Error = crate::pl_row_error::PlRowSerdeError;
    type SerializeSeq = PlSerPlaceHolder2;

    type SerializeTuple = PlSerPlaceHolder2;

    type SerializeTupleStruct = PlSerPlaceHolder2;

    type SerializeTupleVariant = PlSerPlaceHolder2;

    type SerializeMap = PlSerPlaceHolder2;

    type SerializeStruct = PlRowSerStruct;

    type SerializeStructVariant = PlSerPlaceHolder2;

    impl_as_todo!(
        serialize_bool, bool;
        serialize_i8, i8;
        serialize_i16, i16;
        serialize_i32, i32;
        serialize_i64, i64;
        serialize_u8, u8;
        serialize_u16, u16;
        serialize_u32, u32;
        serialize_u64, u64;
        serialize_f32, f32;
        serialize_f64, f64;
        serialize_char, char;
        serialize_str, &str;
        serialize_bytes, &[u8];
    );

    fn serialize_none(self) -> Result<Self::Ok, Self::Error> {
        todo!()
    }

    fn serialize_some<T>(self, value: &T) -> Result<Self::Ok, Self::Error>
    where
        T: ?Sized + Serialize,
    {
        todo!()
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
        Ok(PlRowSerStruct::default())
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

impl SerializeStruct for PlRowSerStruct {
    type Ok = PlRowSerStruct;

    type Error = crate::PlRowSerdeError;

    fn serialize_field<T>(&mut self, key: &'static str, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.pl_ser_map.entry(key).or_insert_with(|| PlRowSer {
            pl_ser: Series::new_null(key, 0),
        });
        let ser = if let Some(opts) = self.pl_ser_map.remove(key) {
            opts
        } else {
            PlRowSer {
                pl_ser: Series::new_null(key, 0),
            }
        };
        let pl_ser = value.serialize(ser).unwrap();
        self.pl_ser_map.insert(key, PlRowSer { pl_ser });
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self)
    }
}

#[derive(Default)]
pub struct PlRowSerMap {
    pl_ser_map: HashMap<String, PlRowSer>,
    curr_key: String,
}
impl<'a> SerializeMap for PlRowSerMap {
    type Ok = PlRowSerMap;

    type Error = crate::PlRowSerdeError;

    fn serialize_value<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        let ser = if let Some(opts) = self.pl_ser_map.remove(&self.curr_key) {
            opts
        } else {
            PlRowSer {
                pl_ser: Series::new_null(&self.curr_key, 0),
            }
        };
        let pl_ser = value.serialize(ser).unwrap();
        self.pl_ser_map
            .insert(std::mem::take(&mut self.curr_key), PlRowSer { pl_ser });
        Ok(())
    }
    fn serialize_key<T>(&mut self, key: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + Serialize,
    {
        self.curr_key = key.serialize(PlRowSer::default()).unwrap().to_string();
        self.pl_ser_map
            .entry(self.curr_key.to_string())
            .or_insert_with(|| PlRowSer {
                pl_ser: Series::new_null(&self.curr_key, 0),
            });
        Ok(())
    }

    fn end(self) -> Result<Self::Ok, Self::Error> {
        Ok(self)
    }
}
