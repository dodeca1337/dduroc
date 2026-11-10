//! Bounded, fully tagged field decoding for presentation layers.
use crate::{DecodeError, DecodedValue, DecodedValueLimits};
use serde::ser::{self, Serialize};
use std::fmt;

#[derive(Debug)]
struct Refused;
impl fmt::Display for Refused {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("field decoding exceeded limits or failed")
    }
}
impl std::error::Error for Refused {}
impl ser::Error for Refused {
    fn custom<T: fmt::Display>(_: T) -> Self {
        Self
    }
}
type Result<T> = std::result::Result<T, Refused>;
struct Budget {
    limits: DecodedValueLimits,
    nodes: usize,
    bytes: usize,
}
impl Budget {
    fn bytes(&mut self, bytes: usize) -> Result<()> {
        self.bytes = self.bytes.checked_add(bytes).ok_or(Refused)?;
        if self.bytes > self.limits.max_bytes {
            return Err(Refused);
        }
        Ok(())
    }
    fn node(&mut self, depth: usize) -> Result<()> {
        if depth > self.limits.max_depth || self.nodes >= self.limits.max_nodes {
            return Err(Refused);
        }
        self.nodes += 1;
        self.bytes(std::mem::size_of::<DecodedValue>())
    }
    fn text(&mut self, text: &str) -> Result<String> {
        self.bytes(text.len())?;
        Ok(text.to_owned())
    }
}
struct Encoder<'a> {
    budget: &'a mut Budget,
    depth: usize,
}
impl<'a> Encoder<'a> {
    fn leaf(self, value: DecodedValue) -> Result<DecodedValue> {
        self.budget.node(self.depth)?;
        Ok(value)
    }
    fn container(self) -> Result<Compound<'a>> {
        self.budget.node(self.depth)?;
        Ok(Compound {
            budget: self.budget,
            depth: self.depth,
            items: Vec::new(),
            pairs: Vec::new(),
            key: None,
            variant: None,
            map: false,
        })
    }
}
struct Compound<'a> {
    budget: &'a mut Budget,
    depth: usize,
    items: Vec<DecodedValue>,
    pairs: Vec<(DecodedValue, DecodedValue)>,
    key: Option<DecodedValue>,
    variant: Option<String>,
    map: bool,
}
impl Compound<'_> {
    fn item<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        self.items.push(value.serialize(Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        })?);
        Ok(())
    }
    fn pair<T: ?Sized + Serialize>(&mut self, key: &str, value: &T) -> Result<()> {
        self.key = Some(key.serialize(Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        })?);
        self.value(value)
    }
    fn value<T: ?Sized + Serialize>(&mut self, value: &T) -> Result<()> {
        let key = self.key.take().ok_or(Refused)?;
        self.pairs.push((
            key,
            value.serialize(Encoder {
                budget: self.budget,
                depth: self.depth + 1,
            })?,
        ));
        Ok(())
    }
    fn finish(self) -> Result<DecodedValue> {
        if self.key.is_some() {
            return Err(Refused);
        }
        let value = if self.map {
            DecodedValue::Map(self.pairs)
        } else {
            DecodedValue::Sequence(self.items)
        };
        Ok(match self.variant {
            Some(name) => DecodedValue::Variant {
                name,
                value: Box::new(value),
            },
            None => value,
        })
    }
}
macro_rules! integer {($($method:ident($ty:ty)=>$variant:ident),*)=>{$(fn $method(self,value:$ty)->Result<DecodedValue>{self.leaf(DecodedValue::$variant(value.into()))})*};}
impl<'a> ser::Serializer for Encoder<'a> {
    type Ok = DecodedValue;
    type Error = Refused;
    type SerializeSeq = Compound<'a>;
    type SerializeTuple = Compound<'a>;
    type SerializeTupleStruct = Compound<'a>;
    type SerializeTupleVariant = Compound<'a>;
    type SerializeMap = Compound<'a>;
    type SerializeStruct = Compound<'a>;
    type SerializeStructVariant = Compound<'a>;
    integer!(serialize_i8(i8)=>Signed,serialize_i16(i16)=>Signed,serialize_i32(i32)=>Signed,serialize_i64(i64)=>Signed,serialize_i128(i128)=>Signed,serialize_u8(u8)=>Unsigned,serialize_u16(u16)=>Unsigned,serialize_u32(u32)=>Unsigned,serialize_u64(u64)=>Unsigned,serialize_u128(u128)=>Unsigned);
    fn serialize_bool(self, v: bool) -> Result<DecodedValue> {
        self.leaf(DecodedValue::Bool(v))
    }
    fn serialize_f32(self, v: f32) -> Result<DecodedValue> {
        self.leaf(DecodedValue::Float32(v.to_bits()))
    }
    fn serialize_f64(self, v: f64) -> Result<DecodedValue> {
        self.leaf(DecodedValue::Float64(v.to_bits()))
    }
    fn serialize_char(self, v: char) -> Result<DecodedValue> {
        self.serialize_str(v.encode_utf8(&mut [0; 4]))
    }
    fn serialize_str(self, v: &str) -> Result<DecodedValue> {
        self.budget.node(self.depth)?;
        Ok(DecodedValue::String(self.budget.text(v)?))
    }
    fn serialize_bytes(self, v: &[u8]) -> Result<DecodedValue> {
        self.budget.node(self.depth)?;
        self.budget.bytes(v.len())?;
        Ok(DecodedValue::Bytes(v.to_vec()))
    }
    fn serialize_none(self) -> Result<DecodedValue> {
        self.serialize_unit()
    }
    fn serialize_some<T: ?Sized + Serialize>(self, v: &T) -> Result<DecodedValue> {
        v.serialize(self)
    }
    fn serialize_unit(self) -> Result<DecodedValue> {
        self.leaf(DecodedValue::Null)
    }
    fn serialize_unit_struct(self, _: &'static str) -> Result<DecodedValue> {
        self.serialize_unit()
    }
    fn serialize_unit_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
    ) -> Result<DecodedValue> {
        self.budget.node(self.depth)?;
        let name = self.budget.text(name)?;
        let value = Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        }
        .serialize_unit()?;
        Ok(DecodedValue::Variant {
            name,
            value: Box::new(value),
        })
    }
    fn serialize_newtype_struct<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        v: &T,
    ) -> Result<DecodedValue> {
        v.serialize(self)
    }
    fn serialize_newtype_variant<T: ?Sized + Serialize>(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        v: &T,
    ) -> Result<DecodedValue> {
        self.budget.node(self.depth)?;
        let name = self.budget.text(name)?;
        let value = v.serialize(Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        })?;
        Ok(DecodedValue::Variant {
            name,
            value: Box::new(value),
        })
    }
    fn serialize_seq(self, len: Option<usize>) -> Result<Compound<'a>> {
        if len.is_some_and(|n| {
            n > self
                .budget
                .limits
                .max_nodes
                .saturating_sub(self.budget.nodes)
        }) {
            return Err(Refused);
        }
        self.container()
    }
    fn serialize_tuple(self, len: usize) -> Result<Compound<'a>> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_struct(self, _: &'static str, len: usize) -> Result<Compound<'a>> {
        self.serialize_seq(Some(len))
    }
    fn serialize_tuple_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        len: usize,
    ) -> Result<Compound<'a>> {
        self.budget.node(self.depth)?;
        let name = self.budget.text(name)?;
        let mut out = Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        }
        .serialize_seq(Some(len))?;
        out.variant = Some(name);
        Ok(out)
    }
    fn serialize_map(self, _: Option<usize>) -> Result<Compound<'a>> {
        let mut out = self.container()?;
        out.map = true;
        Ok(out)
    }
    fn serialize_struct(self, _: &'static str, len: usize) -> Result<Compound<'a>> {
        self.serialize_map(Some(len))
    }
    fn serialize_struct_variant(
        self,
        _: &'static str,
        _: u32,
        name: &'static str,
        len: usize,
    ) -> Result<Compound<'a>> {
        self.budget.node(self.depth)?;
        let name = self.budget.text(name)?;
        let mut out = Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        }
        .serialize_struct("", len)?;
        out.variant = Some(name);
        Ok(out)
    }
}
macro_rules! sequence {($($trait:ident,$method:ident),*)=>{$(impl ser::$trait for Compound<'_>{type Ok=DecodedValue;type Error=Refused;fn $method<T:?Sized+Serialize>(&mut self,v:&T)->Result<()>{self.item(v)}fn end(self)->Result<DecodedValue>{self.finish()}})*};}
sequence!(
    SerializeSeq,
    serialize_element,
    SerializeTuple,
    serialize_element,
    SerializeTupleStruct,
    serialize_field,
    SerializeTupleVariant,
    serialize_field
);
impl ser::SerializeMap for Compound<'_> {
    type Ok = DecodedValue;
    type Error = Refused;
    fn serialize_key<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<()> {
        if self.key.is_some() {
            return Err(Refused);
        }
        self.key = Some(v.serialize(Encoder {
            budget: self.budget,
            depth: self.depth + 1,
        })?);
        Ok(())
    }
    fn serialize_value<T: ?Sized + Serialize>(&mut self, v: &T) -> Result<()> {
        self.value(v)
    }
    fn end(self) -> Result<DecodedValue> {
        self.finish()
    }
}
macro_rules! structure {($($trait:ident),*)=>{$(impl ser::$trait for Compound<'_>{type Ok=DecodedValue;type Error=Refused;fn serialize_field<T:?Sized+Serialize>(&mut self,k:&'static str,v:&T)->Result<()>{self.pair(k,v)}fn end(self)->Result<DecodedValue>{self.finish()}})*};}
structure!(SerializeStruct, SerializeStructVariant);

/// Serialize a generated event into an unambiguous tree. Library-owned
/// allocations are bounded during serialization; user Serialize code itself
/// is not an allocation sandbox or an interruptible callback.
pub fn decode_fields<T: Serialize>(
    value: &T,
    limits: DecodedValueLimits,
) -> std::result::Result<DecodedValue, DecodeError> {
    value
        .serialize(Encoder {
            budget: &mut Budget {
                limits,
                nodes: 0,
                bytes: 0,
            },
            depth: 0,
        })
        .map_err(|_| DecodeError)
}

/// Format generated templates into a writer that refuses expansion before append.
pub fn render_bounded(
    arguments: fmt::Arguments<'_>,
    max_bytes: usize,
) -> std::result::Result<String, DecodeError> {
    struct Writer {
        text: String,
        max: usize,
    }
    impl fmt::Write for Writer {
        fn write_str(&mut self, text: &str) -> fmt::Result {
            if text.len() > self.max.saturating_sub(self.text.len()) {
                return Err(fmt::Error);
            }
            self.text.push_str(text);
            Ok(())
        }
    }
    let mut writer = Writer {
        text: String::new(),
        max: max_bytes,
    };
    fmt::write(&mut writer, arguments).map_err(|_| DecodeError)?;
    Ok(writer.text)
}

mod admission {
    use super::*;
    use serde::de::{
        self, DeserializeSeed, EnumAccess, MapAccess, SeqAccess, VariantAccess, Visitor,
    };
    struct Limits {
        limits: DecodedValueLimits,
        nodes: usize,
        bytes: usize,
    }
    impl Limits {
        fn node<E: de::Error>(&mut self, depth: usize) -> std::result::Result<(), E> {
            if depth > self.limits.max_depth || self.nodes >= self.limits.max_nodes {
                return Err(E::custom("decoded structure limit"));
            }
            self.nodes += 1;
            Ok(())
        }
        fn bytes<E: de::Error>(&mut self, n: usize) -> std::result::Result<(), E> {
            self.bytes = self
                .bytes
                .checked_add(n)
                .ok_or_else(|| E::custom("decoded size overflow"))?;
            if self.bytes > self.limits.max_bytes {
                return Err(E::custom("decoded bytes limit"));
            }
            Ok(())
        }
        fn hint<E: de::Error>(&self, hint: Option<usize>) -> std::result::Result<(), E> {
            if hint.is_some_and(|n| n > self.limits.max_nodes.saturating_sub(self.nodes)) {
                return Err(E::custom("decoded sequence limit"));
            }
            Ok(())
        }
    }
    struct D<'a, T> {
        inner: T,
        budget: &'a mut Limits,
        depth: usize,
    }
    struct V<'a, T> {
        inner: T,
        budget: &'a mut Limits,
        depth: usize,
    }
    struct Seed<'a, T> {
        inner: T,
        budget: &'a mut Limits,
        depth: usize,
    }
    impl<'de, T: DeserializeSeed<'de>> DeserializeSeed<'de> for Seed<'_, T> {
        type Value = T::Value;
        fn deserialize<D0: de::Deserializer<'de>>(
            self,
            de: D0,
        ) -> std::result::Result<Self::Value, D0::Error> {
            self.inner.deserialize(D {
                inner: de,
                budget: self.budget,
                depth: self.depth,
            })
        }
    }
    macro_rules! forward {($($name:ident),*)=>{$(fn $name<V0:Visitor<'de>>(self,v:V0)->std::result::Result<V0::Value,T::Error>{self.budget.node(self.depth)?;self.inner.$name(V{inner:v,budget:self.budget,depth:self.depth})})*};}
    impl<'de, T: de::Deserializer<'de>> de::Deserializer<'de> for D<'_, T> {
        type Error = T::Error;
        forward!(
            deserialize_any,
            deserialize_bool,
            deserialize_i8,
            deserialize_i16,
            deserialize_i32,
            deserialize_i64,
            deserialize_i128,
            deserialize_u8,
            deserialize_u16,
            deserialize_u32,
            deserialize_u64,
            deserialize_u128,
            deserialize_f32,
            deserialize_f64,
            deserialize_char,
            deserialize_str,
            deserialize_string,
            deserialize_bytes,
            deserialize_byte_buf,
            deserialize_option,
            deserialize_unit,
            deserialize_seq,
            deserialize_map,
            deserialize_identifier,
            deserialize_ignored_any
        );
        fn deserialize_unit_struct<V0: Visitor<'de>>(
            self,
            n: &'static str,
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.node(self.depth)?;
            self.inner.deserialize_unit_struct(
                n,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn deserialize_newtype_struct<V0: Visitor<'de>>(
            self,
            n: &'static str,
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.node(self.depth)?;
            self.inner.deserialize_newtype_struct(
                n,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn deserialize_tuple<V0: Visitor<'de>>(
            self,
            n: usize,
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.node(self.depth)?;
            self.budget.hint(Some(n))?;
            self.inner.deserialize_tuple(
                n,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn deserialize_tuple_struct<V0: Visitor<'de>>(
            self,
            name: &'static str,
            n: usize,
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.node(self.depth)?;
            self.budget.hint(Some(n))?;
            self.inner.deserialize_tuple_struct(
                name,
                n,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn deserialize_struct<V0: Visitor<'de>>(
            self,
            n: &'static str,
            fields: &'static [&'static str],
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.node(self.depth)?;
            self.budget.hint(Some(fields.len()))?;
            self.inner.deserialize_struct(
                n,
                fields,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn deserialize_enum<V0: Visitor<'de>>(
            self,
            n: &'static str,
            variants: &'static [&'static str],
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.node(self.depth)?;
            self.inner.deserialize_enum(
                n,
                variants,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn is_human_readable(&self) -> bool {
            self.inner.is_human_readable()
        }
    }
    macro_rules! visit {($($method:ident($ty:ty)),*)=>{$(fn $method<E:de::Error>(self,value:$ty)->std::result::Result<Self::Value,E>{self.inner.$method(value)})*};}
    impl<'de, T: Visitor<'de>> Visitor<'de> for V<'_, T> {
        type Value = T::Value;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            self.inner.expecting(f)
        }
        visit!(
            visit_bool(bool),
            visit_i8(i8),
            visit_i16(i16),
            visit_i32(i32),
            visit_i64(i64),
            visit_i128(i128),
            visit_u8(u8),
            visit_u16(u16),
            visit_u32(u32),
            visit_u64(u64),
            visit_u128(u128),
            visit_f32(f32),
            visit_f64(f64),
            visit_char(char)
        );
        fn visit_unit<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            self.inner.visit_unit()
        }
        fn visit_none<E: de::Error>(self) -> std::result::Result<Self::Value, E> {
            self.inner.visit_none()
        }
        fn visit_str<E: de::Error>(self, v: &str) -> std::result::Result<Self::Value, E> {
            self.budget.bytes(v.len())?;
            self.inner.visit_str(v)
        }
        fn visit_borrowed_str<E: de::Error>(
            self,
            v: &'de str,
        ) -> std::result::Result<Self::Value, E> {
            self.budget.bytes(v.len())?;
            self.inner.visit_borrowed_str(v)
        }
        fn visit_string<E: de::Error>(self, v: String) -> std::result::Result<Self::Value, E> {
            self.budget.bytes(v.len())?;
            self.inner.visit_string(v)
        }
        fn visit_bytes<E: de::Error>(self, v: &[u8]) -> std::result::Result<Self::Value, E> {
            self.budget.bytes(v.len())?;
            self.inner.visit_bytes(v)
        }
        fn visit_borrowed_bytes<E: de::Error>(
            self,
            v: &'de [u8],
        ) -> std::result::Result<Self::Value, E> {
            self.budget.bytes(v.len())?;
            self.inner.visit_borrowed_bytes(v)
        }
        fn visit_byte_buf<E: de::Error>(self, v: Vec<u8>) -> std::result::Result<Self::Value, E> {
            self.budget.bytes(v.len())?;
            self.inner.visit_byte_buf(v)
        }
        fn visit_some<D0: de::Deserializer<'de>>(
            self,
            d: D0,
        ) -> std::result::Result<Self::Value, D0::Error> {
            self.inner.visit_some(D {
                inner: d,
                budget: self.budget,
                depth: self.depth + 1,
            })
        }
        fn visit_newtype_struct<D0: de::Deserializer<'de>>(
            self,
            d: D0,
        ) -> std::result::Result<Self::Value, D0::Error> {
            self.inner.visit_newtype_struct(D {
                inner: d,
                budget: self.budget,
                depth: self.depth + 1,
            })
        }
        fn visit_seq<A: SeqAccess<'de>>(self, a: A) -> std::result::Result<Self::Value, A::Error> {
            self.budget.hint(a.size_hint())?;
            self.inner.visit_seq(D {
                inner: a,
                budget: self.budget,
                depth: self.depth + 1,
            })
        }
        fn visit_map<A: MapAccess<'de>>(self, a: A) -> std::result::Result<Self::Value, A::Error> {
            self.budget
                .hint(a.size_hint().and_then(|n| n.checked_mul(2)))?;
            self.inner.visit_map(D {
                inner: a,
                budget: self.budget,
                depth: self.depth + 1,
            })
        }
        fn visit_enum<A: EnumAccess<'de>>(
            self,
            a: A,
        ) -> std::result::Result<Self::Value, A::Error> {
            self.inner.visit_enum(D {
                inner: a,
                budget: self.budget,
                depth: self.depth + 1,
            })
        }
    }
    impl<'de, T: SeqAccess<'de>> SeqAccess<'de> for D<'_, T> {
        type Error = T::Error;
        fn next_element_seed<S: DeserializeSeed<'de>>(
            &mut self,
            s: S,
        ) -> std::result::Result<Option<S::Value>, T::Error> {
            self.inner.next_element_seed(Seed {
                inner: s,
                budget: self.budget,
                depth: self.depth,
            })
        }
        fn size_hint(&self) -> Option<usize> {
            self.inner.size_hint().map(|n| {
                n.min(
                    self.budget
                        .limits
                        .max_nodes
                        .saturating_sub(self.budget.nodes),
                )
            })
        }
    }
    impl<'de, T: MapAccess<'de>> MapAccess<'de> for D<'_, T> {
        type Error = T::Error;
        fn next_key_seed<S: DeserializeSeed<'de>>(
            &mut self,
            s: S,
        ) -> std::result::Result<Option<S::Value>, T::Error> {
            self.inner.next_key_seed(Seed {
                inner: s,
                budget: self.budget,
                depth: self.depth,
            })
        }
        fn next_value_seed<S: DeserializeSeed<'de>>(
            &mut self,
            s: S,
        ) -> std::result::Result<S::Value, T::Error> {
            self.inner.next_value_seed(Seed {
                inner: s,
                budget: self.budget,
                depth: self.depth,
            })
        }
        fn size_hint(&self) -> Option<usize> {
            self.inner.size_hint().map(|n| {
                n.min(
                    self.budget
                        .limits
                        .max_nodes
                        .saturating_sub(self.budget.nodes)
                        / 2,
                )
            })
        }
    }
    impl<'de, 'a, T: EnumAccess<'de>> EnumAccess<'de> for D<'a, T> {
        type Error = T::Error;
        type Variant = D<'a, T::Variant>;
        fn variant_seed<S: DeserializeSeed<'de>>(
            self,
            s: S,
        ) -> std::result::Result<(S::Value, Self::Variant), T::Error> {
            let (value, variant) = self.inner.variant_seed(Seed {
                inner: s,
                budget: self.budget,
                depth: self.depth,
            })?;
            Ok((
                value,
                D {
                    inner: variant,
                    budget: self.budget,
                    depth: self.depth,
                },
            ))
        }
    }
    impl<'de, T: VariantAccess<'de>> VariantAccess<'de> for D<'_, T> {
        type Error = T::Error;
        fn unit_variant(self) -> std::result::Result<(), T::Error> {
            self.budget.node(self.depth)?;
            self.inner.unit_variant()
        }
        fn newtype_variant_seed<S: DeserializeSeed<'de>>(
            self,
            s: S,
        ) -> std::result::Result<S::Value, T::Error> {
            self.inner.newtype_variant_seed(Seed {
                inner: s,
                budget: self.budget,
                depth: self.depth,
            })
        }
        fn tuple_variant<V0: Visitor<'de>>(
            self,
            n: usize,
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.hint(Some(n))?;
            self.inner.tuple_variant(
                n,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
        fn struct_variant<V0: Visitor<'de>>(
            self,
            fields: &'static [&'static str],
            v: V0,
        ) -> std::result::Result<V0::Value, T::Error> {
            self.budget.hint(Some(fields.len()))?;
            self.inner.struct_variant(
                fields,
                V {
                    inner: v,
                    budget: self.budget,
                    depth: self.depth,
                },
            )
        }
    }

    pub fn decode<T: de::DeserializeOwned>(
        bytes: &[u8],
        limits: DecodedValueLimits,
    ) -> std::result::Result<T, DecodeError> {
        if bytes.len() > limits.max_bytes {
            return Err(DecodeError);
        }
        let mut deserializer = postcard::Deserializer::from_bytes(bytes);
        let mut budget = Limits {
            limits,
            nodes: 0,
            bytes: 0,
        };
        let value = T::deserialize(D {
            inner: &mut deserializer,
            budget: &mut budget,
            depth: 0,
        })
        .map_err(|_| DecodeError)?;
        if !deserializer.finalize().map_err(|_| DecodeError)?.is_empty() {
            return Err(DecodeError);
        }
        Ok(value)
    }
}
pub use admission::decode as decode_payload_bounded;

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    fn limits() -> DecodedValueLimits {
        DecodedValueLimits {
            max_depth: 16,
            max_nodes: 128,
            max_bytes: 16 * 1024,
        }
    }
    #[test]
    fn tagged_values_preserve_widest_integers_and_nan_payload_bits() {
        #[derive(Serialize)]
        struct Fields {
            signed: i128,
            unsigned: u128,
            number: f64,
            negative_zero: f32,
        }
        let value = decode_fields(
            &Fields {
                signed: i128::MIN,
                unsigned: u128::MAX,
                number: f64::from_bits(0x7ff8_0000_0000_0001),
                negative_zero: -0.0,
            },
            limits(),
        )
        .unwrap();
        let DecodedValue::Map(fields) = value else {
            panic!("struct must be a tagged map")
        };
        assert_eq!(fields[0].1, DecodedValue::Signed(i128::MIN));
        assert_eq!(fields[1].1, DecodedValue::Unsigned(u128::MAX));
        assert_eq!(fields[2].1, DecodedValue::Float64(0x7ff8_0000_0000_0001));
        assert_eq!(fields[3].1, DecodedValue::Float32(0x8000_0000));
    }
    #[test]
    fn zero_sized_sequence_iteration_stops_at_the_node_budget() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        static VISITED: AtomicUsize = AtomicUsize::new(0);
        struct Probe;
        impl<'de> Deserialize<'de> for Probe {
            fn deserialize<D: serde::Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                struct Visitor;
                impl<'de> serde::de::Visitor<'de> for Visitor {
                    type Value = Probe;
                    fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                        f.write_str("sequence")
                    }
                    fn visit_seq<A: serde::de::SeqAccess<'de>>(
                        self,
                        mut sequence: A,
                    ) -> std::result::Result<Probe, A::Error> {
                        while sequence.next_element::<()>()?.is_some() {
                            VISITED.fetch_add(1, Ordering::Relaxed);
                        }
                        Ok(Probe)
                    }
                }
                deserializer.deserialize_seq(Visitor)
            }
        }
        // Unit items consume no bytes; Postcard's hint is capped by remaining
        // bytes and may be zero. The per-seed guard must still stop the loop.
        let prefix = postcard::to_allocvec(&1_000_000usize).unwrap();
        assert!(decode_payload_bounded::<Probe>(&prefix, limits()).is_err());
        assert!(VISITED.load(Ordering::Relaxed) < limits().max_nodes);
        assert!(decode_payload_bounded::<Vec<()>>(&prefix, limits()).is_err());
    }
    #[test]
    fn recursive_deserialization_and_serialization_share_explicit_depth_caps() {
        #[derive(Serialize, Deserialize)]
        enum Node {
            End,
            Next(Box<Node>),
        }
        let mut value = Node::End;
        for _ in 0..20 {
            value = Node::Next(Box::new(value));
        }
        let bytes = postcard::to_allocvec(&value).unwrap();
        assert!(decode_payload_bounded::<Node>(&bytes, limits()).is_err());
        assert!(decode_fields(&value, limits()).is_err());
        let mut enough = limits();
        enough.max_depth = 64;
        enough.max_nodes = 512;
        assert!(decode_payload_bounded::<Node>(&bytes, enough).is_ok());
    }
    #[test]
    fn formatter_refuses_template_expansion_and_byte_budget_before_append() {
        let value = "x".repeat(1024);
        assert!(render_bounded(format_args!("{value}{value}{value}"), 1025).is_err());
        assert_eq!(render_bounded(format_args!("{}", "é"), 2).unwrap(), "é");
        assert!(render_bounded(format_args!("{}", "é"), 1).is_err());
        let mut budget = limits();
        budget.max_bytes = 16;
        assert!(decode_fields(&"oversized", budget).is_err());
    }
}
