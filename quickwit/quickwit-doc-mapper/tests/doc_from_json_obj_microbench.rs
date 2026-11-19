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

//! E002 pre-check microbench: `DocMapper::doc_from_json_obj` on one representative
//! `otel_logs_10m` document (dynamic mode, `expand_dots`, no `store_source`), exactly the
//! document mapping of `searchbench/quickwit/config/index_config.json`.
//!
//! The measured call is the whole `doc_from_json_obj` call as the ingest path invokes it,
//! i.e. including the `serde_json` object clone the owned signature forces on the caller.
//! `clone_only` isolates that clone so the mapper work can be read off the same run.
//!
//! Run: `cargo test --release -p quickwit-doc-mapper --test doc_from_json_obj_microbench --
//! --nocapture`

use std::hint::black_box;
use std::time::Instant;

use quickwit_doc_mapper::DocMapper;

/// The `doc_mapping` of `searchbench/quickwit/config/index_config.json`.
const DOC_MAPPING: &str = r#"{
    "mode": "dynamic",
    "dynamic_mapping": {
        "indexed": false,
        "stored": true,
        "fast": false,
        "expand_dots": true
    },
    "field_mappings": [
        {
            "name": "Timestamp",
            "type": "datetime",
            "input_formats": [
                "unix_timestamp",
                "rfc3339"
            ],
            "fast_precision": "milliseconds",
            "fast": true,
            "indexed": false
        },
        {
            "name": "Body",
            "type": "text",
            "tokenizer": "default",
            "record": "position"
        },
        {
            "name": "ServiceName",
            "type": "text",
            "tokenizer": "raw",
            "fast": true
        },
        {
            "name": "SeverityText",
            "type": "text",
            "tokenizer": "raw",
            "fast": true
        },
        {
            "name": "ScopeName",
            "type": "text",
            "indexed": false,
            "fast": true
        },
        {
            "name": "SeverityNumber",
            "type": "i64",
            "fast": true
        },
        {
            "name": "TraceId",
            "type": "text",
            "indexed": false
        },
        {
            "name": "SpanId",
            "type": "text",
            "indexed": false
        }
    ],
    "timestamp_field": "Timestamp"
}"#;

/// One representative `otel_logs` document: 1170 bytes, 16 `ResourceAttributes`,
/// 3 `LogAttributes`, matching the corpus medians of `otel_logs_10m`.
const DOC: &str = r#"{"Timestamp":"2025-09-23T00:00:00.000007636Z","TimestampTime":"2025-09-23T00:00:00.000Z","TraceId":"","SpanId":"","TraceFlags":0,"SeverityText":"info","SeverityNumber":9,"ServiceName":"shipping","Body":"23:59:59 [INFO] Received quote: \"0\"","ResourceSchemaUrl":"","ResourceAttributes":{"app":"app","app.kubernetes.io/component":"shipping","app.kubernetes.io/instance":"opentelemetry-demo","app.kubernetes.io/name":"shipping","k8s.container.name":"shipping","k8s.container.restart_count":"0","k8s.deployment.name":"shipping","k8s.namespace.name":"otel-demo","k8s.node.name":"gke-clickstack-demo-2-default-pool-37081102-9sqs","k8s.pod.name":"shipping-bb55d5b4d-wbv9z","k8s.pod.start_time":"2025-09-16T22:00:53Z","k8s.pod.uid":"71a02703-b9e9-4887-ad2d-09d5b3dba58c","kubectl.kubernetes.io/restartedAt":"2025-09-17T00:00:08+02:00","opentelemetry.io/name":"shipping","pod-template-hash":"bb55d5b4d","service.name":"shipping"},"ScopeSchemaUrl":"","ScopeName":"","ScopeVersion":"","ScopeAttributes":{},"LogAttributes":{"log.file.path":"/var/log/pods/otel-demo_shipping-bb55d5b4d-wbv9z_71a02703-b9e9-4887-ad2d-09d5b3dba58c/shipping/0.log","log.iostream":"stdout","logtag":"F"}}"#;

const CALLS_PER_ROUND: usize = 100_000;
const ROUNDS: usize = 8;

fn median(mut values: Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    values[values.len() / 2]
}

#[test]
fn microbench_doc_from_json_obj() {
    let doc_mapper: DocMapper = serde_json::from_str(DOC_MAPPING).unwrap();
    let json_obj: serde_json::Map<String, serde_json::Value> = serde_json::from_str(DOC).unwrap();
    let document_len = DOC.len() as u64;

    let mut call_us_per_round = Vec::with_capacity(ROUNDS);
    let mut clone_us_per_round = Vec::with_capacity(ROUNDS);
    for _ in 0..ROUNDS {
        let start = Instant::now();
        for _ in 0..CALLS_PER_ROUND {
            black_box(doc_mapper.doc_from_json_obj(json_obj.clone(), document_len).unwrap());
        }
        call_us_per_round.push(start.elapsed().as_secs_f64() * 1e6 / CALLS_PER_ROUND as f64);

        let start = Instant::now();
        for _ in 0..CALLS_PER_ROUND {
            black_box(json_obj.clone());
        }
        clone_us_per_round.push(start.elapsed().as_secs_f64() * 1e6 / CALLS_PER_ROUND as f64);
    }

    // Round 0 warms the allocator and branch predictors; it is reported but not aggregated.
    for round in 0..ROUNDS {
        println!(
            "round {round}: call {:.4} us clone_only {:.4} us",
            call_us_per_round[round], clone_us_per_round[round]
        );
    }
    let call_us = median(call_us_per_round[1..].to_vec());
    let clone_us = median(clone_us_per_round[1..].to_vec());
    println!("calls_per_round {CALLS_PER_ROUND} rounds {ROUNDS} doc_bytes {}", DOC.len());
    println!("median_us_per_call {call_us:.4}");
    println!("median_us_per_clone_only {clone_us:.4}");
    println!("median_us_per_call_without_clone {:.4}", call_us - clone_us);
}
