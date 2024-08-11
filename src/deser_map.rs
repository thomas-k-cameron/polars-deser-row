use std::rc::Rc;

use polars::series::Series;
use serde::{
    de::{value::StrDeserializer, IntoDeserializer, MapAccess},
    Deserializer,
};

use crate::{
    deser_root::SeriesDeserItem, deser_series::SeriesDeser, pl_row_error::PlRowSerdeError,
};

pub(crate) struct PlRowImplMapAccess {
    pub stack: Rc<Box<[Series]>>,
    pub map_value_idx: usize,
    pub row_idx: usize,
}

impl<'de> IntoDeserializer<'de> for PlRowImplMapAccess {
    type Deserializer = PlRowImplMapAccess;

    fn into_deserializer(self) -> Self::Deserializer {
        self
    }
}

impl PlRowImplMapAccess {
    pub(crate) fn new(series_deser: &SeriesDeser) -> Self {
        Self {
            stack: Rc::new(series_deser.df.get_columns().to_vec().into_boxed_slice()),
            row_idx: series_deser.row_idx,
            map_value_idx: 0,
        }
    }

    pub fn from_series_vec(stack: Rc<Box<[Series]>>) -> Self {
        Self {
            stack,
            row_idx: 0,
            map_value_idx: 0,
        }
    }
}

impl<'de> MapAccess<'de> for PlRowImplMapAccess {
    type Error = PlRowSerdeError;

    fn next_key<K>(&mut self) -> Result<Option<K>, Self::Error>
    where
        K: serde::Deserialize<'de>,
    {
        match self.stack.get(self.map_value_idx) {
            None => Ok(None),
            Some(got) => K::deserialize(StrDeserializer::new(got.name())).map(|i| Some(i)),
        }
    }

    fn next_value<V>(&mut self) -> Result<V, Self::Error>
    where
        V: serde::Deserialize<'de>,
    {
        let _map_value_idx = self.map_value_idx;
        self.map_value_idx += 1;

        let item = SeriesDeserItem {
            row_idx: self.row_idx,
            // this always succeed because the value exists at next_key
            series: self.stack[_map_value_idx].clone(),
        };

        V::deserialize(item)
    }

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        match self.stack.get(self.map_value_idx) {
            None => Ok(None),
            Some(got) => seed
                .deserialize(StrDeserializer::new(got.name()))
                .map(|i| Some(i)),
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let _map_value_idx = self.map_value_idx;
        self.map_value_idx += 1;

        let item = SeriesDeserItem {
            row_idx: self.row_idx,
            // this always succeed because the value exists at next_key
            series: self.stack[_map_value_idx].clone(),
        };

        seed.deserialize(item)
    }
}

impl<'de> Deserializer<'de> for PlRowImplMapAccess {
    // only function to implement
    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visitor.visit_map(self)
    }

    type Error = PlRowSerdeError;

    // all the other stuff
    fn deserialize_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bool<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_i64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u8<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u16<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_u64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f32<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_f64<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_char<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_str<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_string<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_bytes<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_byte_buf<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_option<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_unit_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        _name: &'static str,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_seq<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple<V>(self, _len: usize, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        _name: &'static str,
        _len: usize,
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_enum<V>(
        self,
        _name: &'static str,
        _variants: &'static [&'static str],
        _visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_identifier<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }

    fn deserialize_ignored_any<V>(self, _visitor: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        todo!()
    }
}
