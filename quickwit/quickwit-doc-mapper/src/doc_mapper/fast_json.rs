// Copyright 2021-Present Datadog, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Schema-guided JSON decoding for ordinary flat documents.
//!
//! The legacy path parses the whole document into a `serde_json::Map` (a `BTreeMap`)
//! and then walks it. For a flat strict/lenient schema that map is overhead the mapper
//! pays and immediately re-reads. This module decodes a document straight into schema
//! slots and materializes the Tantivy document in the legacy lexical field order,
//! installing the grouped sidecar the inverted-index plugin reads.
//!
//! Values are staged in their borrowed, leaf-typed form ([`serde_json_borrow::Value`])
//! and emitted with the leaf-typed `TantivyDocument::add_*` constructors, so an
//! escape-free string value is never copied into an owned `String`/`JsonValue` and never
//! converted through `OwnedValue::as_value`.
//!
//! Eligibility is a property of the *configuration* (see `DocMapper::fast_json_plan`),
//! never of the input, so an unsupported document is routed to the legacy path before
//! any parsing happens — the input is never parsed twice.

use fnv::FnvHashMap;
use serde::Deserializer;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
#[cfg(test)]
use serde_json::Value as JsonValue;
use serde_json_borrow::Value as BorrowedJsonValue;
use tantivy::TantivyDocument as Document;

use super::mapping_tree::{MappingLeaf, MappingNode};
use crate::DocParsingError;

/// One schema field in a [`FastJsonPlan`].
#[derive(Clone)]
struct FastJsonSlot {
    name: String,
    leaf: MappingLeaf,
}

/// A decoding plan for a flat schema.
///
/// `name_to_slot` resolves an input key in O(1); `lexical_slots` lists the slots in
/// ascending field-name order, which is the order the legacy mapper emits fields in
/// (its input is a `BTreeMap`), so the materialized document keeps the legacy root
/// order.
#[derive(Clone)]
pub(crate) struct FastJsonPlan {
    name_to_slot: FnvHashMap<String, usize>,
    slots: Vec<FastJsonSlot>,
    lexical_slots: Vec<usize>,
}

/// Why the fast path rejected a document.
///
/// The two variants map to the two legacy error categories, so a document is counted
/// and reported exactly as it would be by the map walk.
pub enum FastJsonError {
    /// The bytes are not a syntactically valid JSON object.
    Json(serde_json::Error),
    /// The document parsed but is not valid for the schema.
    Schema(DocParsingError),
}

impl FastJsonPlan {
    /// Builds a plan for a flat schema (every branch a leaf with no concatenate or
    /// tie-breaker). Returns `None` for any schema the fast path does not model, so the
    /// caller keeps the legacy map walk.
    pub(crate) fn build(root: &MappingNode) -> Option<FastJsonPlan> {
        let mut slots = Vec::with_capacity(root.branches().len());
        for (name, tree) in root.branches() {
            let leaf = tree.as_leaf()?;
            if !leaf.supports_fast_json() {
                return None;
            }
            slots.push(FastJsonSlot {
                name: name.clone(),
                leaf: leaf.clone(),
            });
        }
        let mut name_to_slot = FnvHashMap::default();
        for (slot_idx, slot) in slots.iter().enumerate() {
            name_to_slot.insert(slot.name.clone(), slot_idx);
        }
        let mut lexical_slots: Vec<usize> = (0..slots.len()).collect();
        lexical_slots.sort_by(|left, right| slots[*left].name.cmp(&slots[*right].name));
        Some(FastJsonPlan {
            name_to_slot,
            slots,
            lexical_slots,
        })
    }

    /// Decodes `bytes` into a Tantivy document, staging each mapped value in its
    /// borrowed leaf-typed form.
    ///
    /// Preserves the legacy accepted values, last-key-wins semantics,
    /// JSON-syntax-before-schema error precedence, the legacy lexical mapping-error order
    /// and the legacy lexical root order.
    pub(crate) fn decode<'de>(
        &self,
        bytes: &'de [u8],
        strict: bool,
    ) -> Result<(u64, Document), FastJsonError> {
        let mut slots: Vec<Option<BorrowedJsonValue<'de>>> = vec![None; self.slots.len()];
        let mut smallest_unknown: Option<String> = None;
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        RootSeed {
            plan: self,
            strict,
            slots: &mut slots,
            smallest_unknown: &mut smallest_unknown,
        }
        .deserialize(&mut deserializer)
        .map_err(FastJsonError::Json)?;
        // Reject trailing non-whitespace, exactly like `serde_json::from_slice`.
        deserializer.end().map_err(FastJsonError::Json)?;

        let mut document = Document::new();
        for &slot_idx in &self.lexical_slots {
            let slot = &self.slots[slot_idx];
            // A strict-mode unknown field errors at its lexical position, before any
            // field that sorts after it, matching the legacy BTreeMap walk.
            if let Some(unknown) = &smallest_unknown {
                if unknown.as_str() < slot.name.as_str() {
                    return Err(FastJsonError::Schema(DocParsingError::NoSuchFieldInSchema(
                        unknown.clone(),
                    )));
                }
            }
            let Some(value) = slots[slot_idx].take() else {
                continue;
            };
            slot.leaf
                .doc_from_borrowed_json_value(value, &mut document, &slot.name, strict)
                .map_err(FastJsonError::Schema)?;
        }
        if let Some(unknown) = smallest_unknown {
            return Err(FastJsonError::Schema(DocParsingError::NoSuchFieldInSchema(
                unknown,
            )));
        }
        // Values were emitted field by field in document order, so the grouped sidecar
        // is built from them without a per-document comparison sort.
        document.seal_groups();
        Ok((0u64, document))
    }

    /// The owned-staging decoder this change replaces, kept for the pre-check
    /// micro-benchmark that measures the borrowed decoder against it. Production only
    /// compiles [`FastJsonPlan::decode`].
    #[cfg(test)]
    pub(crate) fn decode_owned(
        &self,
        bytes: &[u8],
        strict: bool,
    ) -> Result<(u64, Document), FastJsonError> {
        let mut slots: Vec<Option<JsonValue>> = vec![None; self.slots.len()];
        let mut smallest_unknown: Option<String> = None;
        let mut deserializer = serde_json::Deserializer::from_slice(bytes);
        OwnedRootSeed {
            plan: self,
            strict,
            slots: &mut slots,
            smallest_unknown: &mut smallest_unknown,
        }
        .deserialize(&mut deserializer)
        .map_err(FastJsonError::Json)?;
        // Reject trailing non-whitespace, exactly like `serde_json::from_slice`.
        deserializer.end().map_err(FastJsonError::Json)?;

        let mut document = Document::new();
        let mut path: Vec<String> = Vec::new();
        for &slot_idx in &self.lexical_slots {
            let slot = &self.slots[slot_idx];
            if let Some(unknown) = &smallest_unknown {
                if unknown.as_str() < slot.name.as_str() {
                    return Err(FastJsonError::Schema(DocParsingError::NoSuchFieldInSchema(
                        unknown.clone(),
                    )));
                }
            }
            let Some(value) = slots[slot_idx].take() else {
                continue;
            };
            path.clear();
            path.push(slot.name.clone());
            slot.leaf
                .doc_from_json_value(value, &mut document, &mut path, strict)
                .map_err(FastJsonError::Schema)?;
        }
        if let Some(unknown) = smallest_unknown {
            return Err(FastJsonError::Schema(DocParsingError::NoSuchFieldInSchema(
                unknown,
            )));
        }
        document.seal_groups();
        Ok((0u64, document))
    }

    /// The number of schema slots in the plan (the flat field count).
    #[cfg(test)]
    pub(crate) fn slot_count(&self) -> usize {
        self.slots.len()
    }
}

struct RootSeed<'a, 'de> {
    plan: &'a FastJsonPlan,
    strict: bool,
    slots: &'a mut Vec<Option<BorrowedJsonValue<'de>>>,
    smallest_unknown: &'a mut Option<String>,
}

impl<'de> DeserializeSeed<'de> for RootSeed<'_, 'de> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(RootVisitor {
            plan: self.plan,
            strict: self.strict,
            slots: self.slots,
            smallest_unknown: self.smallest_unknown,
        })
    }
}

struct RootVisitor<'a, 'de> {
    plan: &'a FastJsonPlan,
    strict: bool,
    slots: &'a mut Vec<Option<BorrowedJsonValue<'de>>>,
    smallest_unknown: &'a mut Option<String>,
}

impl<'de> Visitor<'de> for RootVisitor<'_, 'de> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            match self.plan.name_to_slot.get(&key) {
                Some(&slot_idx) => {
                    // Last key wins, like the legacy `serde_json::Map` insert.
                    self.slots[slot_idx] = Some(map.next_value::<BorrowedJsonValue<'de>>()?);
                }
                None => {
                    // The value is unmapped; skip it without allocating. In strict mode
                    // remember the lexically smallest unknown key and raise it during
                    // emission, so the error order matches the legacy walk.
                    map.next_value::<IgnoredAny>()?;
                    if self.strict {
                        let is_smaller = match self.smallest_unknown.as_ref() {
                            Some(current) => key < *current,
                            None => true,
                        };
                        if is_smaller {
                            *self.smallest_unknown = Some(key);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

/// The owned-staging visitor of [`FastJsonPlan::decode_owned`], kept only for the
/// pre-check micro-benchmark.
#[cfg(test)]
struct OwnedRootSeed<'a> {
    plan: &'a FastJsonPlan,
    strict: bool,
    slots: &'a mut Vec<Option<JsonValue>>,
    smallest_unknown: &'a mut Option<String>,
}

#[cfg(test)]
impl<'de> DeserializeSeed<'de> for OwnedRootSeed<'_> {
    type Value = ();

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<(), D::Error> {
        deserializer.deserialize_map(OwnedRootVisitor {
            plan: self.plan,
            strict: self.strict,
            slots: self.slots,
            smallest_unknown: self.smallest_unknown,
        })
    }
}

#[cfg(test)]
struct OwnedRootVisitor<'a> {
    plan: &'a FastJsonPlan,
    strict: bool,
    slots: &'a mut Vec<Option<JsonValue>>,
    smallest_unknown: &'a mut Option<String>,
}

#[cfg(test)]
impl<'de> Visitor<'de> for OwnedRootVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        while let Some(key) = map.next_key::<String>()? {
            match self.plan.name_to_slot.get(&key) {
                Some(&slot_idx) => {
                    self.slots[slot_idx] = Some(map.next_value::<JsonValue>()?);
                }
                None => {
                    map.next_value::<IgnoredAny>()?;
                    if self.strict {
                        let is_smaller = match self.smallest_unknown.as_ref() {
                            Some(current) => key < *current,
                            None => true,
                        };
                        if is_smaller {
                            *self.smallest_unknown = Some(key);
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
