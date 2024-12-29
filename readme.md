# Serde for columnar file formats with Polars

This crate offers deserialization polars dataframe to native rust type per-row and vise-versa.
This project is WIP.

## Use Case

1. You want to use columnar file to store you data, instead of using things like ndjson.
2. You want to apply a complicated computation on your data, but you can't figure out how to do it in polars way. So you want to stick with Rust control flow.

## Data type conversion

### Primitive Types

|polars type|rust type|
|--|--|
|Null|Option::None|
|boolean|bool|
|int64|i64|
|int32|i32|
|int16|i16|
|int8|i8|
|uint64|u64|
|uint32|u32|
|uint16|u16|
|uint8|u8|
|float64|f64|
|float32|f32|

### Composite Types

Arrow comes with data types that you can't easily map to a corresponding rust type.

|polars type|rust type|
|--|--|
|utf8|TODO!|
|list|TODO!|
|map|TODO!|

## Examples

### Deserializing Integers and `bool`

```text
---- tests::deser_primitive_integers stdout ----
shape: (3, 9)
┌───────┬──────┬───────┬───────┬───┬───────┬────────┬────────┬────────┐
│ bool  ┆ int8 ┆ int16 ┆ int32 ┆ … ┆ uint8 ┆ uint16 ┆ uint32 ┆ uint64 │
│ ---   ┆ ---  ┆ ---   ┆ ---   ┆   ┆ ---   ┆ ---    ┆ ---    ┆ ---    │
│ bool  ┆ i8   ┆ i16   ┆ i32   ┆   ┆ u8    ┆ u16    ┆ u32    ┆ u64    │
╞═══════╪══════╪═══════╪═══════╪═══╪═══════╪════════╪════════╪════════╡
│ false ┆ 1    ┆ 1     ┆ 1     ┆ … ┆ 1     ┆ 1      ┆ 1      ┆ 1      │
│ true  ┆ 2    ┆ 2     ┆ 2     ┆ … ┆ 2     ┆ 2      ┆ 2      ┆ 2      │
│ false ┆ 3    ┆ 3     ┆ 3     ┆ … ┆ 3     ┆ 3      ┆ 3      ┆ 3      │
└───────┴──────┴───────┴───────┴───┴───────┴────────┴────────┴────────┘
```

This becomes,

```rust
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
```

### Deserializing Float

```text
---- tests::deser_primitive_float stdout ----
shape: (3, 2)
┌─────────┬─────────┐
│ float32 ┆ float64 │
│ ---     ┆ ---     │
│ f32     ┆ f64     │
╞═════════╪═════════╡
│ 1.0     ┆ 1.0     │
│ 2.0     ┆ 2.0     │
│ 3.0     ┆ 3.0     │
└─────────┴─────────┘
```

This becomes,

```rust
 PrimitiveFloat {
    float32: 3.0,
    float64: 3.0,
}
```

## TODO

- [ ] properly support nested structures for serialization
- [ ] properly support nested structures for **de**serialization
