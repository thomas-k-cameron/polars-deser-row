use std::marker::PhantomData;

use serde::de::{self, value::UnitDeserializer, IntoDeserializer};

pub struct ChunkedArrayDeserializer<'de, I, E, IntoD>
where
    E: serde::de::Error,
    IntoD: IntoDeserializer<'de, E>,
    I: Iterator<Item = Option<IntoD>>,
{
    chunked: I,
    len: usize,
    _p: PhantomData<(&'de (), E)>,
}

impl<'de, I, E, IntoD> ChunkedArrayDeserializer<'de, I, E, IntoD>
where
    E: serde::de::Error,
    IntoD: IntoDeserializer<'de, E>,
    I: Iterator<Item = Option<IntoD>>,
{
    pub fn new(chunked: I, len: usize) -> Self {
        Self {
            chunked,
            len,
            _p: PhantomData,
        }
    }
}

impl<'de, E, I, IntoD> serde::de::SeqAccess<'de> for ChunkedArrayDeserializer<'de, I, E, IntoD>
where
    IntoD: IntoDeserializer<'de, E>,
    I: Iterator<Item = Option<IntoD>>,
    E: serde::de::Error,
{
    type Error = E;

    fn next_element_seed<V>(&mut self, seed: V) -> Result<Option<V::Value>, Self::Error>
    where
        V: serde::de::DeserializeSeed<'de>,
    {
        match self.chunked.next() {
            Some(None) => seed.deserialize(UnitDeserializer::new()).map(|i| Some(i)),
            Some(Some(value)) => seed.deserialize(value.into_deserializer()).map(|i| Some(i)),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        Some(self.len)
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NoneDeserializer<E> {
    marker: PhantomData<E>,
}

impl<E> NoneDeserializer<E> {
    #[allow(missing_docs)]
    pub fn new() -> Self {
        NoneDeserializer {
            marker: PhantomData,
        }
    }
}
macro_rules! impl_none {
    ($($funcname: ident),*) => {
        $(
            fn $funcname<V>(self, _: V) -> Result<V::Value, Self::Error>
            where
                V: de::Visitor<'de>,
            {
                unreachable!();
            }
        ) *
    };
}
impl<'de, E> de::Deserializer<'de> for NoneDeserializer<E>
where
    E: de::Error,
{
    type Error = E;

    fn deserialize_any<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_none()
    }

    fn deserialize_option<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        visitor.visit_none()
    }

    impl_none!(
        deserialize_bool,
        deserialize_i8,
        deserialize_i16,
        deserialize_i32,
        deserialize_i64,
        deserialize_u8,
        deserialize_u16,
        deserialize_u32,
        deserialize_u64,
        deserialize_f32,
        deserialize_f64,
        deserialize_char,
        deserialize_str,
        deserialize_string,
        deserialize_bytes,
        deserialize_byte_buf,
        deserialize_unit,
        deserialize_seq,
        deserialize_identifier,
        deserialize_ignored_any
    );

    fn deserialize_unit_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }

    fn deserialize_newtype_struct<V>(
        self,
        name: &'static str,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }

    fn deserialize_tuple<V>(self, len: usize, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }

    fn deserialize_tuple_struct<V>(
        self,
        name: &'static str,
        len: usize,
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }

    fn deserialize_map<V>(self, visitor: V) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }

    fn deserialize_struct<V>(
        self,
        name: &'static str,
        fields: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }

    fn deserialize_enum<V>(
        self,
        name: &'static str,
        variants: &'static [&'static str],
        visitor: V,
    ) -> Result<V::Value, Self::Error>
    where
        V: de::Visitor<'de>,
    {
        unreachable!()
    }
}
