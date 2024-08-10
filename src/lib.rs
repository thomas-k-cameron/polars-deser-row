#![doc = include_str!("./lib.doc.md")]
use deser_series::SeriesDeser;
use pl_row_error::PlRowSerdeError;
use polars::frame::DataFrame;
use ser_root::PlRowSerStruct;
use serde::{de::Error, Deserialize};

//deserialize
pub(crate) mod deser_map;
pub(crate) mod deser_root;
pub(crate) mod deser_seq;
pub(crate) mod deser_series;

// pl row error
pub(crate) mod pl_row_error;

// serialize
pub(crate) mod ser_root;
//pub mod ser_seq;
//pub mod series_serde_root;

/// Deserialize a row from given dataframe.
///
pub fn deserialize_single_row<'de, T>(df: DataFrame, row_idx: usize) -> Result<T, PlRowSerdeError>
where
    T: Deserialize<'de>,
{
    <T as Deserialize>::deserialize(SeriesDeser { df, row_idx })
}

/// Deserialize whole dataframe.
pub fn deserialize_all<'de, T>(df: DataFrame) -> Vec<Result<T, PlRowSerdeError>>
where
    T: Deserialize<'de>,
{
    let mut stack = Vec::with_capacity(df.height());
    for row_idx in 0..df.height() {
        stack.push(<T as Deserialize>::deserialize(SeriesDeser {
            df: df.clone(),
            row_idx,
        }));
    }
    stack
}

/// Serialize rust iterator into a dataframe.
pub fn serialize_into_dataframe<T, I>(iter: I) -> Result<DataFrame, PlRowSerdeError>
where
    I: Iterator<Item = T>,
    T: serde::Serialize,
{
    let mut plr = PlRowSerStruct::default();

    for i in iter {
        let res = i.serialize(plr);
        match res {
            Ok(a) => {
                plr = a;
            }
            Err(e) => return Err(PlRowSerdeError::custom(e)),
        }
    }
    Ok(plr.into_dataframe())
}

/// Serialize a rust type into a dataframe.
pub fn serialize_item_into_dataframe<T>(item: T) -> Result<DataFrame, PlRowSerdeError>
where
    T: serde::Serialize,
{
    serialize_into_dataframe([item].into_iter())
}

#[cfg(test)]
mod tests;
