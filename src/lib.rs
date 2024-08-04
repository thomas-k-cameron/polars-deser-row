use polars::frame::DataFrame;
use serde::Deserialize;
use series_deser::SeriesDeser;
use series_deser_error::SeriesDeserError;

mod seq;
pub mod series_deser;
pub mod series_deser_error;
pub mod series_deser_map;
pub mod series_deser_root;
//pub mod series_serde_root;

pub fn deserialize_from_dataframe<'de, T>(
    df: DataFrame,
    row_idx: usize,
) -> Result<T, SeriesDeserError>
where
    T: Deserialize<'de>,
{
    <T as Deserialize>::deserialize(SeriesDeser { df, row_idx })
}

pub fn from_dataframe_deserialize_all<'de, T>(df: DataFrame) -> Vec<Result<T, SeriesDeserError>>
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
