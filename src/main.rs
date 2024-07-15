use polars_deser_row::series_deser::SeriesDeser;
use serde::Deserialize;

fn main() {
    let df = polars::df! {
        "int8" => [-8i8],
        "int8_to_16" => [-8_16i16],
        "int16" => [-16i16],
        "int32" => [-32i32],
        "int64" => [-64i64],
        "uint8" => [8u8],
        "uint16" => [16u16],
        "uint32" => [32u32],
        "uint64" => [64u64],
        "float32" => [f32::EPSILON],
        "float64" => [f64::EPSILON],
        "utf8" => ["hello".to_string()],
        "bytes_box" => ["hello".as_bytes()],
        "bytes_vec" => ["hello".as_bytes()],
        "bytes_char" => ["c"],
    }
    .unwrap();
    println!("{df}");
    let asdf = Asdf::deserialize(SeriesDeser::new(df, 0)).unwrap();
    println!("{asdf:#?}");
}

#[derive(serde::Deserialize, Debug)]
struct Asdf {
    int8: i8,
    int8_to_16: i16,
    int16: i16,
    int32: i32,
    int64: i64,
    uint8: u8,
    uint16: u16,
    uint32: u32,
    uint64: u64,
    float32: f32,
    float64: f64,
    utf8: String,
    bytes_box: Box<[u8]>,
    bytes_vec: Vec<u8>,
    bytes_char: char,
}

#[derive(serde::Deserialize, Debug)]
enum AsdfEnum {
    Lol,
    Stuff,
    Things,
}
