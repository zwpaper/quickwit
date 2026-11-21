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

use quickwit_common::pubsub::Event;
use quickwit_proto::types::SourceUid;

/// E041: emitted by an [`IngestSource`](crate::source::IngestSource) when it observes that its
/// input has genuinely stopped producing: `IDLE_FORCE_COMMIT_EMPTY_ROUNDS` completed fetch rounds
/// fetched no payload. Unlike the split-publication stream, this signal is not disturbed by a
/// busy indexer/merger (the fetch rounds do not complete when the pipeline is backpressured), so
/// it is the closest engine-observable proxy for "the client stopped sending".
///
/// The event is consumed by the opt-in post-ingest compaction pass. It is emitted at most once
/// per idle episode (until a payload is fetched again).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngestSourceIdle {
    pub source_uid: SourceUid,
}

impl Event for IngestSourceIdle {}
