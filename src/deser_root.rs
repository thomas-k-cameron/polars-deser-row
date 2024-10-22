use std::{collections::HashSet, rc::Rc};

use polars::{
    datatypes::{AnyValue, DataType, LogicalType},
    prelude::NamedFrom,
    series::Series,
};
use serde::{
    de::{
        value::{SeqDeserializer, StrDeserializer},
        Error, IntoDeserializer, Visitor,
    },
    Deserializer,
};

use crate::{
    deser_map::PlRowImplMapAccess, deser_seq::ChunkedArrayDeserializer,
    pl_row_error::PlRowSerdeError,
};

#[derive(Debug)]
pub(crate) struct SeriesDeserItem {
    pub series: Series,
    pub row_idx: usize,
}

impl<'de> IntoDeserializer<'de> for SeriesDeserItem {
    type Deserializer = SeriesDeserItem;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

impl SeriesDeserItem {
    fn maybe_list_or_bytes<'de, V>(self, visitor: V) -> Result<V::Value, PlRowSerdeError>
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
                    .map_err(|e| PlRowSerdeError::custom(e))?
                    .get(self.row_idx)
                    .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                    .and_then(|i| visitor.visit_bytes(i))
            }
            DataType::List(_) => self.deserialize_seq(visitor),
            _ => unimplemented!(),
        }
    }
}

impl<'de> Deserializer<'de> for SeriesDeserItem {
    type Error = PlRowSerdeError;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // try date
        unimplemented!()
    }

    fn deserialize_bool<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.bool();
        match res {
            Ok(i) => i
                .get(self.row_idx)
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_bool(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_i8(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_i16(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_i32(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_i64(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_u8(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_u16(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
        }
    }

    fn deserialize_u32<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.u32();
        match res {
            Ok(i) => i
                .get(self.row_idx)
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_u32(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_u64(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_f32(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_f64(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
        }
    }

    fn deserialize_char<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        // try str

        let c = 'c: {
            let s_u8 = self.series.u8();
            let opt_c = s_u8.map(|i| i.get(self.row_idx).map(|i| i as char));
            if let Ok(Some(c)) = opt_c {
                break 'c c;
            }

            let res = self.series.str().map(|i| i.get(self.row_idx));
            match res {
                Ok(Some(i)) if i.len() == 1 => {
                    if let Some(c) = i.as_bytes().get(0).map(|i| *i as char) {
                        break 'c c;
                    };
                }
                Ok(None) | Ok(Some(_)) => {
                    return Err(PlRowSerdeError::custom(source_code_location::new_string!()))
                }
                Err(e) => return Err(PlRowSerdeError::custom(e)),
            };

            return Err(PlRowSerdeError::custom("msg"));
        };

        visitor.visit_char(c)
    }

    fn deserialize_str<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        let res = self.series.str();
        match res {
            Ok(i) => i
                .get(self.row_idx)
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_str(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
        }
    }

    fn deserialize_string<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_str(visitor)
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
                    .map_err(|e| PlRowSerdeError::custom(e))?
                    .get(self.row_idx)
                    .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
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
                .ok_or_else(|| PlRowSerdeError::custom(source_code_location::new_string!()))
                .and_then(|i| visitor.visit_bytes(i)),
            Err(e) => Err(PlRowSerdeError::custom(e)),
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
                Err(e) => Err(PlRowSerdeError::custom(e)),
            }
        } else {
            visitor.visit_some(self)
        }
    }

    fn deserialize_unit<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_bool(true)
    }

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_str(name)
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_str(name)
    }

    fn deserialize_seq<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        macro_rules! seq_deser {
            ($self: ident, $visitor: ident, $chunked_array: ident) => {
                if let Some(seq) = $chunked_array.get(self.row_idx) {
                    visitor.visit_seq(SeqDeserializer::new(seq.into_iter().map(|i| *i)))
                } else {
                    let arr: [u8; 0] = [];
                    visitor.visit_seq(SeqDeserializer::new(arr.into_iter()))
                }
            };
        }
        // try binary
        match self.series.binary() {
            Ok(chunked_array) => {
                let res = seq_deser!(self, visitor, chunked_array);
                return res;
            }
            // skip
            Err(_) => (),
        };
        // binary offset
        match self.series.binary_offset() {
            Ok(chunked_array) => {
                let res = seq_deser!(self, visitor, chunked_array);
                return res;
            }
            // skip
            Err(_) => (),
        };
        // try str
        match self.series.str() {
            Ok(chunked_array) => {
                let res = if let Some(seq) = chunked_array.get(self.row_idx) {
                    visitor.visit_seq(SeqDeserializer::new(seq.as_bytes().into_iter().map(|i| *i)))
                } else {
                    return Err(PlRowSerdeError::custom(source_code_location::new_string!()));
                };
                return res;
            }
            // skip
            Err(_) => (),
        };

        let res = self.series.list();
        match res {
            Ok(i) => {
                let series = i.get_as_series(self.row_idx).unwrap();
                macro_rules! template {
                    ($dt: ident, $series: ident) => {
                        visitor.visit_seq(ChunkedArrayDeserializer::new(
                            $series.$dt().unwrap().iter(),
                            $series.len(),
                        ))
                    };
                }

                macro_rules! template_time_stuff {
                    ($ser_func: ident, $series: ident) => {
                        visitor.visit_seq({
                            let dt = $series.$ser_func().unwrap();
                            let seq = ChunkedArrayDeserializer::new(dt.into_iter(), dt.len());
                            seq
                        })
                    };
                }

                match series.dtype() {
                    // PRIMITIVE START
                    DataType::Float32 => template!(f32, series),
                    DataType::Float64 => template!(f64, series),
                    DataType::Boolean => template!(bool, series),
                    DataType::UInt8 => template!(u8, series),
                    DataType::UInt16 => template!(u16, series),
                    DataType::UInt32 => template!(u32, series),
                    DataType::UInt64 => template!(u64, series),
                    DataType::Int8 => template!(i8, series),
                    DataType::Int16 => template!(i16, series),
                    DataType::Int32 => template!(i32, series),
                    DataType::Int64 => template!(i64, series),
                    // END PRIMITIVE
                    DataType::Decimal(_, _) => template!(decimal, series),
                    DataType::String => template!(str, series),
                    DataType::Binary => template!(binary, series),
                    DataType::BinaryOffset => template!(binary_offset, series),
                    DataType::Date => template_time_stuff!(date, series),
                    DataType::Datetime(_, _) => template_time_stuff!(datetime, series),
                    DataType::Duration(_) => template_time_stuff!(duration, series),
                    DataType::Time => template_time_stuff!(time, series),
                    DataType::Array(_, size) => {
                        let iter = series
                            .array()
                            .unwrap()
                            .into_iter()
                            .map(|opt_ser| {
                                (0..series.len()).into_iter().map(move |row_idx| {
                                    if let Some(series) = opt_ser.as_ref() {
                                        Some(SeriesDeserItem {
                                            series: series.clone(),
                                            row_idx,
                                        })
                                    } else {
                                        None
                                    }
                                })
                            })
                            .flatten();

                        visitor.visit_seq(ChunkedArrayDeserializer::new(iter, *size))
                    }
                    DataType::List(_) => {
                        let c = ChunkedArrayDeserializer::new(
                            series.list().unwrap().into_iter().map(|i| match i {
                                None => None,
                                Some(series) => Some(SeriesDeserItem { series, row_idx: 0 }),
                            }),
                            series.len(),
                        );
                        visitor.visit_seq(c)
                    }
                    DataType::Null => visitor.visit_seq(ChunkedArrayDeserializer::new(
                        (0..self.series.len())
                            .into_iter()
                            .map(|_| Option::<()>::None),
                        self.series.len(),
                    )),
                    // categoricals
                    DataType::Categorical(_, _) | DataType::Enum(_, _) => {
                        visitor.visit_seq(ChunkedArrayDeserializer::new(
                            series.categorical().unwrap().iter_str(),
                            self.series.len(),
                        ))
                    }
                    DataType::Struct(_) => {
                        let c = self.series.struct_().unwrap();
                        let stack = Rc::new(c.fields().to_owned().into_boxed_slice());
                        let iter = (0..c.len()).into_iter().map(|row_idx| {
                            Some(PlRowImplMapAccess {
                                stack: stack.clone(),
                                map_value_idx: 0,
                                row_idx,
                            })
                        });

                        let deser = ChunkedArrayDeserializer::new(iter, self.series.len());
                        visitor.visit_seq(deser)
                    }
                    // todo. Fix this
                    DataType::Object(_, _) => todo!(),
                    DataType::Unknown(_) => todo!(),
                }
            }
            Err(e) => Err(PlRowSerdeError::custom(e)),
        }
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match len {
            0 => visitor.visit_unit(),
            1 => todo!(),
            _ => todo!(),
        }
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        self.deserialize_tuple(len, visitor)
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        match self.series.struct_() {
            Ok(s) => {
                let map = PlRowImplMapAccess::from_series_vec(Rc::new(
                    s.fields().to_vec().into_boxed_slice(),
                ));
                visitor.visit_map(map)
            }
            Err(e) => Err(PlRowSerdeError::custom(e.to_string())),
        }
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
        match self.series.struct_() {
            Ok(s) => {
                if let Some(i) = s.iter().skip(self.row_idx).next() {
                } else {
                };
                let map = PlRowImplMapAccess::from_series_vec(Rc::new(
                    s.fields().to_vec().into_boxed_slice(),
                ));
                visitor.visit_map(map)
            }
            Err(e) => Err(PlRowSerdeError::custom(e.to_string())),
        }
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
        match self.series.categorical() {
            Ok(cat) => {
                let opts = cat.iter_str().skip(self.row_idx).next();
                match opts {
                    None | Some(None) => visitor.visit_none(),
                    Some(Some(opt)) => visitor.visit_enum(StrDeserializer::new(opt)),
                }
            }
            Err(e) => Err(PlRowSerdeError::custom(e)),
        }
    }

    fn deserialize_identifier<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_newtype_struct(self)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_unit()
    }
}
