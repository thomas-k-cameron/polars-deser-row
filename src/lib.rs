use deser_series::SeriesDeser;
use pl_row_error::PlRowSerdeError;
use polars::frame::DataFrame;
use serde::Deserialize;

//deserialize
pub mod deser_map;
pub mod deser_root;
pub mod deser_seq;
pub mod deser_series;
pub mod pl_row_error;

// serialize
pub mod ser_root;
pub mod ser_seq;
//pub mod series_serde_root;

pub fn deserialize_from_dataframe<'de, T>(
    df: DataFrame,
    row_idx: usize,
) -> Result<T, PlRowSerdeError>
where
    T: Deserialize<'de>,
{
    <T as Deserialize>::deserialize(SeriesDeser { df, row_idx })
}

pub fn from_dataframe_deserialize_all<'de, T>(df: DataFrame) -> Vec<Result<T, PlRowSerdeError>>
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

#[cfg(test)]
mod tests;
