use std::collections::HashMap;

use serde::ser::{SerializeSeq, SerializeTuple};

use crate::ser_root::SerdeItemAnyValueBuffer;

pub struct PlRowSerializeSeq<'a> {
    inner: Option<SerdeItemAnyValueBuffer<'a>>,
}
impl<'a> SerializeSeq for PlRowSerializeSeq<'a> {
    type Ok = SerdeItemAnyValueBuffer<'a>;

    type Error = crate::PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        match self.inner.take() {
            Some(item) => {
                let item = value.serialize(item)?;
                self.inner = Some(item)
            }
            None => unreachable!(),
        };

        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        match self.inner {
            Some(mut item) => {
                item.change_not_null_anyvalue_buffer(None);
                Ok(item)
            }
            None => unreachable!(),
        }
    }
}
