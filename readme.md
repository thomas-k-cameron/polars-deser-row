# Serde for columnar file formats with Polars

This crate offers deserialization polars dataframe to native rust type per-row and vise-versa.

Columnar file formats can dramatically reduce the disk usage but it has been  rather difficult to use it with ease.

This project is WIP.

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
