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
//! Eligibility is a property of the *configuration* (see `DocMapper::fast_json_plan`),
//! never of the input, so an unsupported document is routed to the legacy path before
//! any parsing happens — the input is never parsed twice.

use std::hash::Hasher;

use fnv::FnvHashMap;
use serde::de::{DeserializeSeed, IgnoredAny, MapAccess, Visitor};
use serde::Deserializer;
use serde_json::Value as JsonValue;
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
/// `key_index` maps the FNV-1a fingerprint of a field name to its slot, so a borrowed
/// input key resolves without allocating; `lexical_slots` lists the slots in ascending
/// field-name order, which is the order the legacy mapper emits fields in (its input is
/// a `BTreeMap`), so the materialized document keeps the legacy root order.
#[derive(Clone)]
pub(crate) struct FastJsonPlan {
    key_index: FnvHashMap<u64, u32>,
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
        Self::build_with(root, fnv1a)
    }

    /// [`FastJsonPlan::build`] with an injectable fingerprint, so a test can force a
    /// collision; production always passes [`fnv1a`].
    pub(crate) fn build_with(
        root: &MappingNode,
        fingerprint: fn(&str) -> u64,
    ) -> Option<FastJsonPlan> {
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
        let mut key_index: FnvHashMap<u64, u32> = FnvHashMap::default();
        for (slot_idx, slot) in slots.iter().enumerate() {
            // Two field names with the same fingerprint cannot be told apart by a
            // borrowed key, so decline the whole plan and keep the legacy map walk.
            if key_index
                .insert(fingerprint(&slot.name), slot_idx as u32)
                .is_some()
            {
                return None;
            }
        }
        let mut lexical_slots: Vec<usize> = (0..slots.len()).collect();
        lexical_slots.sort_by(|left, right| slots[*left].name.cmp(&slots[*right].name));
        Some(FastJsonPlan {
            key_index,
            slots,
            lexical_slots,
        })
    }

    /// Resolves a borrowed input key to its slot, without allocating.
    fn resolve(&self, key: &str) -> Option<u32> {
        self.resolve_with(key, fnv1a)
    }

    /// [`FastJsonPlan::resolve`] with an injectable fingerprint, so a test can check the
    /// name verification against a forced collision; production always passes [`fnv1a`].
    pub(crate) fn resolve_with(&self, key: &str, fingerprint: fn(&str) -> u64) -> Option<u32> {
        let &slot_idx = self.key_index.get(&fingerprint(key))?;
        // A fingerprint is not injective over arbitrary input keys: verify the name so
        // an unknown key that merely shares a fingerprint is not mapped to a field.
        if self.slots[slot_idx as usize].name == key {
            Some(slot_idx)
        } else {
            None
        }
    }

    /// Decodes `bytes` into a Tantivy document.
    ///
    /// Preserves the legacy accepted values (the leaf conversion is reused verbatim),
    /// last-key-wins semantics, JSON-syntax-before-schema error precedence, the legacy
    /// lexical mapping-error order and the legacy lexical root order.
    pub(crate) fn decode(
        &self,
        bytes: &[u8],
        strict: bool,
    ) -> Result<(u64, Document), FastJsonError> {
        let mut slots: Vec<Option<JsonValue>> = vec![None; self.slots.len()];
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
        let mut path: Vec<String> = Vec::new();
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
        // Values were emitted field by field in document order, so the grouped sidecar
        // is built from them without a per-document comparison sort.
        document.seal_groups();
        Ok((0u64, document))
    }
}

struct RootSeed<'a> {
    plan: &'a FastJsonPlan,
    strict: bool,
    slots: &'a mut Vec<Option<JsonValue>>,
    smallest_unknown: &'a mut Option<String>,
}

impl<'de> DeserializeSeed<'de> for RootSeed<'_> {
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

/// The FNV-1a 64-bit fingerprint of a field name, the key of [`FastJsonPlan::key_index`].
fn fnv1a(name: &str) -> u64 {
    let mut hasher = fnv::FnvHasher::default();
    hasher.write(name.as_bytes());
    hasher.finish()
}

struct RootVisitor<'a> {
    plan: &'a FastJsonPlan,
    strict: bool,
    slots: &'a mut Vec<Option<JsonValue>>,
    smallest_unknown: &'a mut Option<String>,
}

impl<'de> Visitor<'de> for RootVisitor<'_> {
    type Value = ();

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object")
    }

    fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
        loop {
            let Some(resolved) = map.next_key_seed(KeySeed {
                plan: self.plan,
                strict: self.strict,
                smallest_unknown: &mut *self.smallest_unknown,
            })?
            else {
                break;
            };
            match resolved {
                Some(slot_idx) => {
                    // Last key wins, like the legacy `serde_json::Map` insert.
                    self.slots[slot_idx as usize] = Some(map.next_value::<JsonValue>()?);
                }
                None => {
                    // The value is unmapped; skip it. `KeySeed` recorded a strict-mode
                    // unknown key if it was the lexically smallest so far.
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(())
    }
}

/// Resolves one object key against the plan's fingerprint index without allocating.
///
/// `serde_json` hands an unescaped key to `visit_borrowed_str`, so the common case never
/// copies it; an escaped key is unescaped into a scratch buffer and arrives at
/// `visit_str`. A key that does not name a schema field is skipped, and in strict mode
/// the lexically smallest unknown key is remembered (allocating only when it wins), so
/// the legacy error order is preserved.
pub(crate) struct KeySeed<'a> {
    pub(crate) plan: &'a FastJsonPlan,
    pub(crate) strict: bool,
    pub(crate) smallest_unknown: &'a mut Option<String>,
}

impl<'de> DeserializeSeed<'de> for KeySeed<'_> {
    type Value = Option<u32>;

    fn deserialize<D: Deserializer<'de>>(self, deserializer: D) -> Result<Self::Value, D::Error> {
        deserializer.deserialize_str(KeyVisitor {
            plan: self.plan,
            strict: self.strict,
            smallest_unknown: self.smallest_unknown,
        })
    }
}

struct KeyVisitor<'a> {
    plan: &'a FastJsonPlan,
    strict: bool,
    smallest_unknown: &'a mut Option<String>,
}

impl<'de> Visitor<'de> for KeyVisitor<'_> {
    type Value = Option<u32>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        formatter.write_str("a JSON object key")
    }

    fn visit_borrowed_str<E: serde::de::Error>(self, key: &'de str) -> Result<Self::Value, E> {
        Ok(self.resolve_or_record(key))
    }

    fn visit_str<E: serde::de::Error>(self, key: &str) -> Result<Self::Value, E> {
        Ok(self.resolve_or_record(key))
    }
}

impl KeyVisitor<'_> {
    fn resolve_or_record(self, key: &str) -> Option<u32> {
        if let Some(slot_idx) = self.plan.resolve(key) {
            return Some(slot_idx);
        }
        if self.strict {
            let is_smaller = match self.smallest_unknown.as_ref() {
                Some(current) => key < current.as_str(),
                None => true,
            };
            if is_smaller {
                *self.smallest_unknown = Some(key.to_owned());
            }
        }
        None
    }
}
