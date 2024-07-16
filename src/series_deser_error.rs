pub type SeriesDeserError = serde::de::value::Error;
//
// #[derive(Debug)]
// pub enum SeriesDeserError {
//     TypeMisMatch,
//     CharLength,
//     Null,
//     PolarsError(polars::error::PolarsError),
//     Custom(String),
// }
// impl std::fmt::Display for SeriesDeserError {
//     fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
//         f.write_str("ok")
//     }
// }
// impl serde::ser::StdError for SeriesDeserError {}
// impl serde::de::Error for SeriesDeserError {
//     fn custom<T>(msg: T) -> Self
//     where
//         T: std::fmt::Display,
//     {
//         Self::Custom(msg.to_string())
//     }
// }
//

#[macro_export]
macro_rules! custom_error {
    ($s: literal) => {
        let scl = source_code_location::new_string!();
        serde::de::value::Error::custom(format!("error at {scl}. Detail: {$s}"))
    };
}
