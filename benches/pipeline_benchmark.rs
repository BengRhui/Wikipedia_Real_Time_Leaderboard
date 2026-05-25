//! benches/pipeline_benchmarks.rs
//!
//! This is the benchmark file that compares the performance of different tasks in the program.

use criterion::{black_box, criterion_group, criterion_main, Criterion, Throughput};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use wiki_rt::config::{Safety, CHANNEL_CAPACITY, HIGH_PRIORITY_QUOTA};
use wiki_rt::display::Dashboard;
use wiki_rt::fault_tolerance::failsafe::FailSafe;
use wiki_rt::leaderboard::create_leaderboard;
use wiki_rt::monitor::{get_alloc_bytes, get_alloc_count, CountingAllocator};
use wiki_rt::pipeline::queue_async::PriorityQueueAsync;
use wiki_rt::pipeline::queue_threaded::PriorityQueueThread;
use wiki_rt::processor::{is_bot_edit, process_event};
use wiki_rt::types::{WikiEdit, WikiEvent};

use bytes::Bytes;
use serde::Deserialize;

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

/// Constants to assist in benchmarking
const BATCH: usize = 128;
const PRODUCERS: usize = 4;
const EVENTS_PER_PRODUCER: usize = 256;
const LATENCY_SAMPLES: usize = 256;
const CONTENTION_THREADS: usize = 4;
const CONTENTION_OPS: usize = 2000;
const SSE: &[u8] = b"data:{\"type\":\"edit\",\"bot\":false,\"meta\":{\"domain\":\"en.wikipedia.org\"}}\n\n";
const JSON: &[u8] = b"{\"type\":\"edit\",\"bot\":false,\"meta\":{\"domain\":\"en.wikipedia.org\"}}";

/// Some structs to help in benchmarking
#[derive(Debug, Deserialize)]
struct OwnedEvent {
    #[serde(default)]
    bot: Option<bool>,
    #[serde(default, rename = "type")]
    change_type: Option<String>,
    #[serde(default)]
    meta: Option<OwnedMeta>,
}

#[derive(Debug, Deserialize)]
struct OwnedMeta {
    domain: Option<String>,
}

/// Some helper functions to help in benchmarking
fn dashboard() -> Arc<Dashboard> {
    Dashboard::new(create_leaderboard(&Safety::Mutex))
}

async fn ingest_async(raw: &[u8]) -> Option<WikiEdit> {
    let bot = is_bot_edit(raw)?;
    Some(WikiEdit::new(bot, Bytes::from_static(SSE)))
}

fn ingest_thread(raw: &[u8]) -> Option<WikiEdit> {
    let bot = is_bot_edit(raw)?;
    Some(WikiEdit::new(bot, Bytes::from_static(SSE)))
}

fn p99(v: &mut [u128]) -> u128 {
    v.sort_unstable();
    v[(v.len() * 99) / 100]
}

/// The benchmark function that measures the overall performance (throughput) of the pipeline
/// This includes ingestion, queueing, and end-to-end performance
fn bench_pipeline(c: &mut Criterion) {

    // Setup the benchmark group and let this measure throughput
    let mut g = c.benchmark_group("01_pipeline");
    g.throughput(Throughput::Elements(BATCH as u64));

    // Initialize worker pool for tasks
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .unwrap();

    // Ingestion pipeline for async (tokio)
    g.bench_function("async_ingest", |b| {
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..BATCH {
                    let _ = ingest_async(black_box(SSE)).await;
                }
            })
        })
    });

    // Ingestion pipeline for thread
    g.bench_function("thread_ingest", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                let _ = ingest_thread(black_box(SSE));
            }
        })
    });

    // Initiate the different queues and failsafe
    let async_queue = PriorityQueueAsync::new(CHANNEL_CAPACITY, HIGH_PRIORITY_QUOTA, dashboard());
    let thread_queue = PriorityQueueThread::new(CHANNEL_CAPACITY, HIGH_PRIORITY_QUOTA, dashboard());
    let failsafe = Arc::new(FailSafe::new());

    // Measuring how async queue performs under contentions (multiple producers + one consumer access one queue)
    g.bench_function("async_queue_contention", |b| {
        b.iter(|| {
            rt.block_on(async {
                let total = PRODUCERS * EVENTS_PER_PRODUCER;

                // Consumer task
                let consumer = {
                    let q = Arc::clone(&async_queue);
                    tokio::spawn(async move {
                        for _ in 0..total {
                            let _ = q.recv().await;
                        }
                    })
                };

                // Producer tasks
                let producers: Vec<_> = (0..PRODUCERS)
                    .map(|_| {
                        let q = Arc::clone(&async_queue);
                        let f = Arc::clone(&failsafe);
                        tokio::spawn(async move {
                            for _ in 0..EVENTS_PER_PRODUCER {
                                let e = WikiEdit::new(false, Bytes::from_static(SSE));
                                q.send(e, &f).await;
                            }
                        })
                    })
                    .collect();

                // Wait for all tasks to finish
                for p in producers {
                    p.await.unwrap();
                }
                consumer.await.unwrap();
            })
        })
    });

    // Similar for thread: Measuring how multiple producers and one consumer access the queue
    g.bench_function("thread_queue_contention", |b| {
        b.iter(|| {
            let total = PRODUCERS * EVENTS_PER_PRODUCER;

            // Consumer thread
            let consumer = {
                let q = Arc::clone(&thread_queue);
                thread::spawn(move || {
                    for _ in 0..total {
                        let _ = q.recv();
                    }
                })
            };

            // Producer threads
            let producers: Vec<_> = (0..PRODUCERS)
                .map(|_| {
                    let q = Arc::clone(&thread_queue);
                    let f = Arc::clone(&failsafe);
                    thread::spawn(move || {
                        for _ in 0..EVENTS_PER_PRODUCER {
                            let e = WikiEdit::new(false, Bytes::from_static(SSE));
                            q.send(e, &f);
                        }
                    })
                })
                .collect();

            // Wait for all threads to finish
            for p in producers {
                p.join().unwrap();
            }
            consumer.join().unwrap();
        })
    });

    // Measuring the entire async pipeline from ingestion -> queueing -> processing
    g.bench_function("async_pipeline_throughput", |b| {
        b.iter(|| {
            rt.block_on(async {
                for _ in 0..BATCH {
                    if let Some(e) = ingest_async(SSE).await {
                        async_queue.send(e, &failsafe).await;
                        if let Some(d) = async_queue.recv().await {
                            let _ = process_event(&d.payload);
                        }
                    }
                }
            })
        })
    });

    // Similarly, measure the entire thread pipeline from ingestion -> queueing -> processing
    g.bench_function("thread_pipeline_throughput", |b| {
        b.iter(|| {
            for _ in 0..BATCH {
                if let Some(e) = ingest_thread(SSE) {
                    thread_queue.send(e, &failsafe);
                    if let Some(d) = thread_queue.recv() {
                        let _ = process_event(&d.payload);
                    }
                }
            }
        })
    });

    // Measure the latency of the async pipeline (from ingestion to processing) in overall
    g.bench_function("async_latency_p99", |b| {
        b.iter(|| {
            let mut samples = Vec::with_capacity(LATENCY_SAMPLES);

            rt.block_on(async {
                for _ in 0..LATENCY_SAMPLES {
                    let start = Instant::now();

                    if let Some(e) = ingest_async(SSE).await {
                        async_queue.send(e, &failsafe).await;
                        if let Some(d) = async_queue.recv().await {
                            let _ = process_event(&d.payload);
                        }
                    }
                    samples.push(start.elapsed().as_micros());
                }
            });
            black_box(p99(&mut samples));
        })
    });

    // Similarly, measure the latency of the thread pipeline (from ingestion to processing) in overall
    g.bench_function("thread_latency_p99", |b| {
        b.iter(|| {
            let mut samples = Vec::with_capacity(LATENCY_SAMPLES);

            for _ in 0..LATENCY_SAMPLES {
                let start = Instant::now();

                if let Some(e) = ingest_thread(SSE) {
                    thread_queue.send(e, &failsafe);
                    if let Some(d) = thread_queue.recv() {
                        let _ = process_event(&d.payload);
                    }
                }
                samples.push(start.elapsed().as_micros());
            }
            black_box(p99(&mut samples));
        })
    });

    // Done
    g.finish();
}

/// A benchmark function to compare the performance of different synchronization models
fn bench_sync(c: &mut Criterion) {

    // Initialize the benchmark group with different models
    let mut g = c.benchmark_group("02_sync_models");
    let models = [
        ("mutex", Safety::Mutex),
        ("rwlock", Safety::RwLock),
        ("atomic", Safety::Atomic),
        ("dashmap", Safety::DashMap),
    ];

    // Loop through each model
    for (name, mode) in models {

        // Create the leaderboard for each model
        let lb = create_leaderboard(&mode); // reuse (important)

        // Perform update operation towards the leaderboard
        g.bench_function(name, |b| {
            b.iter(|| {
                let handles: Vec<_> = (0..CONTENTION_THREADS)
                    .map(|_| {
                        let lb = Arc::clone(&lb);
                        thread::spawn(move || {
                            for _ in 0..CONTENTION_OPS {
                                lb.update("en.wikipedia.org");
                            }
                        })
                    })
                    .collect();

                // Wait for all tasks to finish
                for h in handles {
                    h.join().unwrap();
                }
            })
        });
    }

    // Done
    g.finish();
}

/// A benchmark function to compare the performance of different memory allocation strategies
fn bench_memory(c: &mut Criterion) {

    // Initialize the benchmark group
    let mut g = c.benchmark_group("03_memory");

    // This one is for usual allocation, where the data is written to the system
    g.bench_function("owned_alloc", |b| {
        b.iter(|| {
            let before_c = get_alloc_count();
            let before_b = get_alloc_bytes();

            for _ in 0..BATCH {
                let _: Result<OwnedEvent, _> =
                    serde_json::from_slice(black_box(JSON));
            }

            black_box((
                get_alloc_count() - before_c,
                get_alloc_bytes() - before_b,
            ));
        })
    });

    // This one is for borrowed allocation, where the data is borrowed from the JSON
    g.bench_function("borrowed_alloc", |b| {
        b.iter(|| {
            let before_c = get_alloc_count();
            let before_b = get_alloc_bytes();

            for _ in 0..BATCH {
                let _: Result<WikiEvent<'_>, _> =
                    serde_json::from_slice(black_box(JSON));
            }

            black_box((
                get_alloc_count() - before_c,
                get_alloc_bytes() - before_b,
            ));
        })
    });

    // Done
    g.finish();
}

// Lastly compile everything and run the benchmarks
criterion_group!(benches, bench_pipeline, bench_sync, bench_memory);
criterion_main!(benches);
