use polars::series::Series;
use serde::de::{value::StrDeserializer, MapAccess};

use crate::{series_deser_error::SeriesDeserError, SeriesDeserItem};

pub struct ImplMapAccess {
    stack: Vec<Series>,
    map_value_idx: usize,
    cell_idx: usize,
}

impl<'de> MapAccess<'de> for ImplMapAccess {
    type Error = SeriesDeserError;

    fn next_key<K>(&mut self) -> Result<Option<K>, Self::Error>
    where
        K: serde::Deserialize<'de>,
    {
        let res = match self.stack.get(self.map_value_idx) {
            None => Ok(None),
            Some(got) => Ok(Some(StrDeserializer::new(got.name()))),
        };

        res
    }

    fn next_value<V>(&mut self) -> Result<V, Self::Error>
    where
        V: serde::Deserialize<'de>,
    {
        let _map_value_idx = self.map_value_idx;
        self.map_value_idx += 1;

        Ok(SeriesDeserItem {
            idx: self.cell_idx,
            // this always succeed because the value exists at next_key
            series: self.stack[_map_value_idx],
        })
    }

    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        self.next_key()
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        self.next_value()
    }
}
