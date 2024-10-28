use polars::df;
use polars_deser_row::{deserialize_all, deserialize_single_row};

fn main() {
    #[derive(serde::Deserialize, Debug, PartialEq, Eq)]
    struct PrimitiveTyInt {
        bool: bool,
        int8: i8,
        int16: i16,
        int32: i32,
        int64: i64,
        uint8: u8,
        uint16: u16,
        uint32: u32,
        uint64: u64,
    }

    let df = df!(
        "bool" => [false, true, false],
        "int8" => [1i8, 2, 3],
        "int16" => [1i16, 2, 3],
        "int32" => [1i32, 2, 3],
        "int64" => [1i64, 2, 3],
        "uint8" => [1u8, 2, 3],
        "uint16" => [1u16, 2, 3],
        "uint32" => [1u32, 2, 3],
        "uint64" => [1u64, 2, 3],
    )
    .unwrap();

    let ty: Vec<PrimitiveTyInt> = crate::deserialize_all(df.clone())
        .into_iter()
        .map(|i| i.unwrap())
        .collect();
    println!("{ty:?}");
    println!("{df}");
}
