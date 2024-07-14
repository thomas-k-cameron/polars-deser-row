use std::{borrow::Cow, marker::PhantomData, slice::SliceIndex, str::Bytes, sync::Arc};

use polars::{
    chunked_array::{
        builder::{AnonymousListBuilder, AnonymousOwnedListBuilder},
        iterator::SomeIterator,
    },
    datatypes::{AnyValue, DataType, Field, Int16Type},
    frame::{
        row::{AnyValueBuffer, Row},
        DataFrame,
    },
    prelude::{ListBuilderTrait, NamedFrom, NoNull},
    series::{self, Series, SeriesIter, SeriesPhysIter, SeriesTrait},
};
use seq::{ChunkedArrayDeserializer, OptDeserializer, OptF32Deserializer, SeqOptDeserializer};
use serde::{
    de::{
        value::{
            CowStrDeserializer, F32Deserializer, I16Deserializer, SeqDeserializer, StrDeserializer,
            UnitDeserializer,
        },
        DeserializeOwned, IntoDeserializer, MapAccess, SeqAccess,
    },
    Deserializer,
};

fn main() {
    let df = polars::df! {
        //"i8" => [1i8],
        //"i16" => [1i16],
        "int32" => [2i32],
        "int64" => [2i64],
        "uint32" => [2u32],
        "uint64" => [2u64],
        "float32" => [f32::EPSILON],
        "float64" => [f64::EPSILON],
        "utf8" => ["hello".to_string()],
        "bytes" => ["hello".as_bytes()]
    }
    .unwrap();
    println!("{df}");
    let row = df.get_row(0).unwrap();
    let deser = DeserPolarsRow {
        row,
        fields: &df.fields(),
        nullable: &df
            .get_columns()
            .iter()
            .map(|i| i.null_count() != 0)
            .collect::<Box<[bool]>>(),
        col_idx: 0,
    };
    let deser = <Asdf as serde::Deserialize>::deserialize(deser).unwrap();
    println!("Hello, world!");
    println!("{deser:?}");

    for i in df.iter() {
        let s = i.i64().unwrap();
        for i in s.iter() {
            s.get(0)
        }
        let s = i.cast(&polars::datatypes::DataType::Int64).unwrap();
    }
}

struct SeriesDeserAsSeq {
    series: Series,
    dt: DataType,
    idx: usize,
}
impl<'de> SeqAccess<'de> for SeriesDeserAsSeq {
    type Error = DeserPolarsRowError;
    fn next_element_seed<T>(&mut self, seed: T) -> Result<Option<T::Value>, Self::Error>
    where
        T: serde::de::DeserializeSeed<'de>,
    {
    }
}

struct SeriesDeserItem {
    pub series: Series,
    pub idx: usize,
}

struct DataFramePerRowSer {
    df: DataFrame,
    idx: usize,
    col_idx: usize,
    state: DataFramePerRowSerState,
}

enum DataFramePerRowSerState {
    Key,
    Value,
}

impl<'de> MapAccess<'de> for DataFramePerRowSer {
    type Error = SeriesDeserError;
    fn next_key_seed<K>(&mut self, seed: K) -> Result<Option<K::Value>, Self::Error>
    where
        K: serde::de::DeserializeSeed<'de>,
    {
        let opts = self.df.get_columns().get(self.col_idx);
        dbg!(opts);
        if let Some(i) = opts {
            let s = i.name();
            seed.deserialize(StrDeserializer::new(s)).map(|i| Some(i))
        } else {
            Ok(None)
        }
    }

    fn next_value_seed<V>(&mut self, seed: V) -> Result<V::Value, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        let series = self
            .df
            .get_columns()
            .get(self.col_idx)
            .ok_or(DeserPolarsRowError::AnyValueError)?;

        seed.deserialize(SeriesDeserItem {
            series: series.clone(),
        })
    }
}

macro_rules! polars_deser_template {
    ($self_var: ident, $visitor: ident, $dt:ident) => {
        match $self_var.series.$dt() {
            Ok(i) => i
                .get($self_var.idx)
                .ok_or_else(|| unimplemented!())
                .and_then(|i| $visitor.concat_idents!(visit_, $dt)(i)),
            Err(e) => Err(SeriesDeserError::PolarsError(e)),
        }
    };
}

mod row_iter;
mod seq;
pub(crate) mod series_deser_error;
pub(crate) mod series_deser_map;
pub(crate) mod series_deser_root;
