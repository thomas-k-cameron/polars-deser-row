use polars::{
    datatypes::{AnyValue, DataType},
    series::Series,
};
use serde::{de::value::MapDeserializer, Deserializer};

use crate::{
    seq::ChunkedArrayDeserializer,
    series_deser_error::{self, SeriesDeserError},
};

struct SeriesDeserItem {
    series: Series,
    idx: usize,
}

impl<'de> Deserializer<'de> for SeriesDeserItem {
    type Error = SeriesDeserError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.bool();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_bool(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_i8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.i8();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_i8(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_i16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.i16();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_i16(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_i32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.i32();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_i32(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_i64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.i64();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_i64(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_u8<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.u8();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_u8(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_u16<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.u16();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_u16(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.u32();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_u32(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_u64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.u64();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_u64(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_f32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.f32();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_f32(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_f64<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.f64();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_f64(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.str();
        match res {
            Ok(i) => {
                let res = i.get(self.idx);
                let ret = match res {
                    Some(c) if c.len() == 1 => c.chars().next(),
                    Some(_) => return Err(SeriesDeserError::CharLength),
                    None => SeriesDeserError::Null,
                };
                ret.ok_or_else(|| SeriesDeserError::Null)
                    .and_then(|i| visitor.visit_char(i))
            }
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.str();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_str(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.str();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_string(i.to_string())),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.binary();
        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_bytes(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.binary();

        match res {
            Ok(i) => i
                .get(self.idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_bytes(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.null_count() > 0;
        if res {
            match self.series.get(self.idx) {
                Ok(AnyValue::Null) => visitor.visit_none(),
                Ok(_) => visitor.visit_some(self),
                Err(e) => Err(SeriesDeserError::PolarsError(e)),
            }
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.list();
        match res {
            Ok(i) => {
                let i = i.get_as_series(self.idx).unwrap();
                macro_rules! template {
                    ($dt: ident, $self: ident) => {
                        visitor.visit_seq(ChunkedArrayDeserializer::new(
                            $self.series.$dt().unwrap().iter(),
                            $self.series.len(),
                        ))
                    };
                }
                match i.dtype() {
                    // PRIMITIVE START
                    DataType::Float32 => template!(f32, self),
                    DataType::Float64 => template!(f64, self),
                    DataType::Boolean => template!(bool, self),
                    DataType::UInt8 => template!(u8, self),
                    DataType::UInt16 => template!(u16, self),
                    DataType::UInt32 => template!(u32, self),
                    DataType::UInt64 => template!(u64, self),
                    DataType::Int8 => template!(i8, self),
                    DataType::Int16 => template!(i16, self),
                    DataType::Int32 => template!(i32, self),
                    DataType::Int64 => template!(i64, self),
                    // END PRIMITIVE
                    DataType::Decimal(_, _) => template!(decimal, self),
                    DataType::String => template!(str, self),
                    DataType::Binary => template!(binary, self),
                    DataType::BinaryOffset => template!(binary, self),
                    DataType::Date => todo!(),
                    DataType::Datetime(_, _) => todo!(),
                    DataType::Duration(_) => todo!(),
                    DataType::Time => todo!(),
                    DataType::Array(_, _) => todo!(),
                    DataType::List(_) => todo!(),
                    DataType::Object(_, _) => todo!(),
                    DataType::Null => visitor.visit_seq(ChunkedArrayDeserializer::new(
                        (0..self.series.len()).into_iter().map(|i| Some(i)),
                        self.series.len(),
                    )),
                    DataType::Categorical(_, _) => todo!(),
                    DataType::Enum(a, _) => {
                        todo!()
                    }
                    DataType::Struct(_) => {
                        todo!()
                    }
                    DataType::Unknown(_) => todo!(),
                }
            }
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_enum(self)
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}
