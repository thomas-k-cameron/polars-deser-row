use std::marker::PhantomData;

use serde::de::{value::UnitDeserializer, IntoDeserializer};

pub struct ChunkedArrayDeserializer<'de, I, E, IntoD>
where
    E: serde::de::Error,
    IntoD: IntoDeserializer<'de, E>,
    I: Iterator<Item = Option<IntoD>>,
{
    chunked: I,
    len: Option<usize>,
    _p: PhantomData<(&'de (), E)>,
}

impl<'de, I, E, IntoD> ChunkedArrayDeserializer<'de, I, E, IntoD>
where
    E: serde::de::Error,
    IntoD: IntoDeserializer<'de, E>,
    I: Iterator<Item = Option<IntoD>>,
{
    pub fn new(chunked: I, len: impl Into<Option<usize>>) -> Self {
        Self {
            chunked,
            len: len.into(),
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
            Some(None) => seed.deserialize(UnitDeserializer::new()).map(|_| None),
            Some(Some(value)) => seed.deserialize(value.into_deserializer()).map(|i| Some(i)),
            None => Ok(None),
        }
    }

    fn size_hint(&self) -> Option<usize> {
        self.len
    }
}
