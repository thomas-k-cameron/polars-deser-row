use polars::series::Series;
use serde::{
    de::{value::StrDeserializer, MapAccess},
    Deserialize,
};

use crate::{
    series_deser::SeriesDeser, series_deser_error::SeriesDeserError,
    series_deser_root::SeriesDeserItem,
};

pub(crate) struct ImplMapAccess {
    stack: Vec<Series>,
    map_value_idx: usize,
    row_idx: usize,
}

impl ImplMapAccess {
    pub(crate) fn new(series_deser: &SeriesDeser) -> Self {
        Self {
            stack: series_deser.df.get_columns().to_vec(),
            row_idx: series_deser.row_idx,
            map_value_idx: 0,
        }
    }

    pub fn from_series_vec(stack: Vec<Series>) -> Self {
        Self {
            stack,
            row_idx: 0,
            map_value_idx: 0,
        }
    }
}

impl<'de> MapAccess<'de> for ImplMapAccess {
    type Error = SeriesDeserError;

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
