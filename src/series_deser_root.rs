use polars::{
    datatypes::{AnyValue, DataType, LogicalType},
    series::Series,
};
use serde::{
    de::{
        value::{EnumAccessDeserializer, SeqDeserializer},
        EnumAccess, Visitor,
    },
    Deserialize, Deserializer,
};

use crate::{seq::ChunkedArrayDeserializer, series_deser_error::SeriesDeserError};

pub(crate) struct SeriesDeserItemDeserialize(SeriesDeserItem);
impl<'de> Deserialize<'de> for SeriesDeserItemDeserialize {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
    }
}

#[derive(Debug)]
pub(crate) struct SeriesDeserItem {
    pub series: Series,
    pub row_idx: usize,
}
impl SeriesDeserItem {
    fn maybe_list_or_bytes<'de, V>(self, visitor: V) -> Result<V::Value, SeriesDeserError>
    where
        V: Visitor<'de>,
    {
        match self.series.dtype() {
            DataType::BinaryOffset | DataType::Binary => {
                let res = self.series.binary();
                if res.is_err() {
                    unreachable!();
                }
                self.series
                    .binary()
                    .map_err(|e| SeriesDeserError::PolarsError(e))?
                    .get(self.row_idx)
                    .ok_or_else(|| SeriesDeserError::Null)
                    .and_then(|i| visitor.visit_bytes(i))
            }
            DataType::List(_) => self.deserialize_seq(visitor),
            _ => unimplemented!(),
        }
    }
}

impl<'de> Deserializer<'de> for SeriesDeserItem {
    type Error = SeriesDeserError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // try date
        if let Ok(list) = self.series.date() {
            return visitor.visit_i32(list.0.get(self.row_idx).unwrap());
        };

        if let Ok(list) = self.series.time() {
            return visitor.visit_i64(list.0.get(self.row_idx).unwrap());
        };

        if let Ok(list) = self.series.datetime() {
            return visitor.visit_i64(list.0.get(self.row_idx).unwrap());
        };

        if let Ok(list) = self.series.duration() {
            return visitor.visit_i64(list.0.get(self.row_idx).unwrap());
        };

        if let Ok(list) = self.series.decimal() {
            return visitor.visit_i128(list.0.get(self.row_idx).unwrap());
        };

        if let Ok(list) = self.series.struct_() {};

        Err(SeriesDeserError::TypeMisMatch)
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.bool();
        match res {
            Ok(i) => i
                .get(self.row_idx)
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
                .get(self.row_idx)
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
                .get(self.row_idx)
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
                .get(self.row_idx)
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
                .get(self.row_idx)
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
                .get(self.row_idx)
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
                .get(self.row_idx)
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
            Ok(i) => {
                dbg!(&res, &self, &i.get(self.row_idx));
                i.get(self.row_idx)
                    .ok_or_else(|| SeriesDeserError::Null)
                    .and_then(|i| visitor.visit_u32(i))
            }
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
                .get(self.row_idx)
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
                .get(self.row_idx)
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
                .get(self.row_idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_f64(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // try str
        let res = self.series.str();
        match res {
            Ok(i) => {
                let res = i.get(self.row_idx);
                let ret = match res {
                    Some(c) if c.len() == 1 => c.chars().next(),
                    Some(_) => return Err(SeriesDeserError::CharLength),
                    None => return Err(SeriesDeserError::CharLength),
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
                .get(self.row_idx)
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
                .get(self.row_idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_string(i.to_string())),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_bytes<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.series.dtype() {
            DataType::BinaryOffset | DataType::Binary => {
                let res = self.series.binary();
                if res.is_err() {
                    unreachable!();
                }
                self.series
                    .binary()
                    .map_err(|e| SeriesDeserError::PolarsError(e))?
                    .get(self.row_idx)
                    .ok_or_else(|| SeriesDeserError::Null)
                    .and_then(|i| visitor.visit_bytes(i))
            }
            DataType::List(_) => self.deserialize_seq(visitor),
            _ => unimplemented!(),
        }
    }

    fn deserialize_byte_buf<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.binary();
        match res {
            Ok(i) => i
                .get(self.row_idx)
                .ok_or_else(|| SeriesDeserError::Null)
                .and_then(|i| visitor.visit_bytes(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        if let Ok(_) = self.series.null() {
            return visitor.visit_none();
        };

        let nul_c = self.series.null_count() > 0;
        if nul_c {
            match self.series.get(self.row_idx) {
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
        // check bytes
        match self.series.binary() {
            Ok(i) => {
                let res = if let Some(seq) = i.get(self.row_idx) {
                    visitor.visit_seq(SeqDeserializer::new(seq.into_iter().map(|i| *i)))
                } else {
                    let arr: [u8; 0] = [];
                    visitor.visit_seq(SeqDeserializer::new(arr.into_iter()))
                };
                return res;
            }
            // skip
            Err(_) => (),
        };
        let res = self.series.list();
        match res {
            Ok(i) => {
                let i = i.get_as_series(self.row_idx).unwrap();
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
                        (0..self.series.len())
                            .into_iter()
                            .map(|_| Option::<()>::None),
                        self.series.len(),
                    )),
                    // categoricals
                    DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                        visitor.visit_seq(ChunkedArrayDeserializer::new(
                            i.categorical().unwrap().iter_str(),
                            self.series.len(),
                        ))
                    }
                    DataType::Enum(None, _) => {
                        todo!()
                    }
                    DataType::Struct(fields) => {
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
        todo!()
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
        todo!()
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
        todo!()
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
        visitor.visit_unit()
    }
}
