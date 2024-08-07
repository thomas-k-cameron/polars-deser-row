use std::fmt::Debug;
use std::io::Cursor;

use polars::datatypes::StructChunked;
use polars::io::SerReader;
use polars::lazy::dsl::{self as pl, SpecialEq};
use polars::prelude::LiteralValue;
use polars::{
    df,
    frame::DataFrame,
    prelude::{IntoLazy, NamedFrom},
    series::Series,
};

#[test]
fn deser_primitive_integers() {
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

    let ty: PrimitiveTyInt = crate::deserialize_from_dataframe(df.clone(), 0).unwrap();
    assert_eq!(
        ty,
        PrimitiveTyInt {
            bool: false,
            int8: 1,
            int16: 1,
            int32: 1,
            int64: 1,
            uint8: 1,
            uint16: 1,
            uint32: 1,
            uint64: 1,
        }
    );
    dbg!(ty);
    let ty: PrimitiveTyInt = crate::deserialize_from_dataframe(df.clone(), 1).unwrap();
    assert_eq!(
        ty,
        PrimitiveTyInt {
            bool: true,
            int8: 2,
            int16: 2,
            int32: 2,
            int64: 2,
            uint8: 2,
            uint16: 2,
            uint32: 2,
            uint64: 2,
        }
    );

    let ty: PrimitiveTyInt = crate::deserialize_from_dataframe(df.clone(), 2).unwrap();
    assert_eq!(
        ty,
        PrimitiveTyInt {
            bool: false,
            int8: 3,
            int16: 3,
            int32: 3,
            int64: 3,
            uint8: 3,
            uint16: 3,
            uint32: 3,
            uint64: 3,
        }
    );

    let ty_stack = crate::from_dataframe_deserialize_all::<PrimitiveTyInt>(df.clone())
        .into_iter()
        .map(|i| i.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        ty_stack,
        vec![
            PrimitiveTyInt {
                bool: false,
                int8: 1,
                int16: 1,
                int32: 1,
                int64: 1,
                uint8: 1,
                uint16: 1,
                uint32: 1,
                uint64: 1,
            },
            PrimitiveTyInt {
                bool: true,
                int8: 2,
                int16: 2,
                int32: 2,
                int64: 2,
                uint8: 2,
                uint16: 2,
                uint32: 2,
                uint64: 2,
            },
            PrimitiveTyInt {
                bool: false,
                int8: 3,
                int16: 3,
                int32: 3,
                int64: 3,
                uint8: 3,
                uint16: 3,
                uint32: 3,
                uint64: 3,
            }
        ]
    )
}

#[test]
fn deser_primitive_float() {
    #[derive(serde::Deserialize, Debug, PartialEq)]
    struct PrimitiveFloat {
        float32: f32,
        float64: f64,
    }
    let df = df!(
        "float32" => [1f32, 2., 3.],
        "float64" => [1f64, 2., 3.],
    )
    .unwrap();

    let ty: PrimitiveFloat = crate::deserialize_from_dataframe(df.clone(), 0).unwrap();
    assert_eq!(
        ty,
        PrimitiveFloat {
            float32: 1.,
            float64: 1.
        }
    );

    let ty: PrimitiveFloat = crate::deserialize_from_dataframe(df.clone(), 1).unwrap();
    assert_eq!(
        ty,
        PrimitiveFloat {
            float32: 2.,
            float64: 2.
        }
    );

    let ty: PrimitiveFloat = crate::deserialize_from_dataframe(df.clone(), 2).unwrap();
    assert_eq!(
        ty,
        PrimitiveFloat {
            float32: 3.,
            float64: 3.
        }
    );
    println!("{df:?}");
    dbg!(ty);
    let ty_stack = crate::from_dataframe_deserialize_all::<PrimitiveFloat>(df.clone())
        .into_iter()
        .map(|i| i.unwrap())
        .collect::<Vec<_>>();

    assert_eq!(
        ty_stack,
        vec![
            PrimitiveFloat {
                float32: 1.,
                float64: 1.
            },
            PrimitiveFloat {
                float32: 2.,
                float64: 2.
            },
            PrimitiveFloat {
                float32: 3.,
                float64: 3.
            }
        ]
    )
}

#[test]
fn deser_seq() {
    macro_rules! template {
        ($struct_name: ident, $ty: ty, $arr1: expr, $arr2: expr) => {
            #[derive(serde::Deserialize, Debug, PartialEq)]
            struct $struct_name {
                veced: Vec<$ty>,
                boxed: Box<[$ty]>,
            }

            let arr_1 = $arr1;
            let arr_2 = $arr2;

            let [veced, boxed] = [
                pl::lit(LiteralValue::Series(SpecialEq::new(Series::new(
                    "veced",
                    [
                        Series::new("", arr_1.clone()),
                        Series::new("", arr_2.clone()),
                    ],
                )))),
                pl::col("veced").alias(&"boxed"),
            ];
            let df = DataFrame::empty()
                .lazy()
                .with_column(veced)
                .with_column(boxed)
                .collect()
                .unwrap();

            let asdf: $struct_name = crate::deserialize_from_dataframe(df.clone(), 0).unwrap();

            assert_eq!(
                asdf,
                $struct_name {
                    veced: arr_1.to_vec(),
                    boxed: arr_1.to_vec().into_boxed_slice(),
                }
            );

            let asdf: $struct_name = crate::deserialize_from_dataframe(df.clone(), 1).unwrap();
            assert_eq!(
                asdf,
                $struct_name {
                    veced: arr_2.to_vec(),
                    boxed: arr_2.to_vec().into_boxed_slice(),
                }
            );
        };
    }

    // int
    template!(Structi8, i8, [1i8, 2, 3], [1, 2, 3]);
    template!(Structi16, i16, [1i16, 2, 3], [1, 2, 3]);
    template!(Structi32, i32, [1i32, 2, 3], [1, 2, 3]);
    template!(Structi64, i64, [1i64, 2, 3], [1i64, 2, 3]);

    // unsigned
    template!(Structu8, u8, [1u8, 2, 3], [1, 2, 3]);
    template!(Structu16, u16, [1u16, 2, 3], [1, 2, 3]);
    template!(Structu32, u32, [1u32, 2, 3], [1, 2, 3]);
    template!(Structu64, u64, [1u64, 2, 3], [1, 2, 3]);

    // float
    template!(
        Structf32,
        f32,
        [f32::NEG_INFINITY, 1f32, 2., 3.],
        [f32::MIN_POSITIVE, 1., 2., 3.]
    );
    template!(
        Structf64,
        f64,
        [f64::NEG_INFINITY, 1f64, 2., 3.],
        [f64::MIN_POSITIVE, 1., 2., 3.]
    );

    template!(StructZeroLength, f64, [], []);
}

#[test]
fn deser_nested_seq() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
    struct Item {
        int8: i8,
        int16: i16,
    }

    let s = StructChunked::new(
        "name",
        &[
            Series::new("int8", [1i8, 2, 3]),
            Series::new("int16", [1i16, 2, 3]),
        ],
    )
    .unwrap();
    println!("{}", s.len());
    println!("{:#?}", s.fields());

    let df = DataFrame::new(s.fields().to_vec()).unwrap();
    let ty: Vec<Result<Item, _>> = crate::from_dataframe_deserialize_all(df);
    println!("{:#?}", ty);
}

#[test]
fn deser_nested_seq_csv() {
    #[derive(serde::Deserialize, serde::Serialize, Debug, PartialEq, Clone)]
    struct Item {
        index: i64,
        customer_id: String,
        first_name: String,
        last_name: String,
        company: String,
        city: String,
        country: String,
        phone_1: String,
        phone_2: String,
        email: String,
        subscription_date: String,
        website: String,
    }
    let df = {
        polars::io::csv::read::CsvReader::new(Cursor::new(include_bytes!("./test-assets/test.csv")))
            .finish()
            .unwrap()
    };

    println!("{:#?}", df.height());

    let df = DataFrame::new(df.get_columns().to_vec()).unwrap();
    let ty: Vec<Result<Item, _>> = crate::from_dataframe_deserialize_all(df);
    println!("{:#?}", ty);
}
