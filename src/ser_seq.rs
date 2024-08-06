use std::collections::HashMap;

use serde::ser::SerializeSeq;

use crate::ser_root::SerdeItemAnyValueBuffer;

pub struct PlRowSerializeSeq<'a> {
    inner: SerdeItemAnyValueBuffer<'a>,
}
impl<'a> SerializeSeq for PlRowSerializeSeq<'a> {
    type Ok = SerdeItemAnyValueBuffer<'a>;

    type Error = crate::PlRowSerdeError;

    fn serialize_element<T>(&mut self, value: &T) -> Result<(), Self::Error>
    where
        T: ?Sized + serde::Serialize,
    {
        value.serialize(&mut self.inner)?;
        Ok(())
    }
    fn end(self) -> Result<Self::Ok, Self::Error> {
        self.inner
            .change_not_null_anyvalue_buffer(self.inner.datatype);
        Ok(())
    }
}
