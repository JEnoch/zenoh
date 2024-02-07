//
// Copyright (c) 2023 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//
use clap::Parser;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};
use zenoh::config::Config;
use zenoh::prelude::sync::*;
use zenoh::publication::CongestionControl;
use zenoh_examples::CommonArgs;

fn main() {
    // initiate logging
    env_logger::init();

    let (config, warmup, size, n) = parse_args();
    let session = zenoh::open(config).res().unwrap();

    // The key expression to publish data on
    let key_expr_ping = keyexpr::new("test/ping").unwrap();

    // The key expression to wait the response back
    let key_expr_pong = keyexpr::new("test/pong").unwrap();

    let sub = session.declare_subscriber(key_expr_pong).res().unwrap();
    let publisher = session
        .declare_publisher(key_expr_ping)
        .congestion_control(CongestionControl::Block)
        .res()
        .unwrap();

    // The key expression to receive parallel traffic
    let key_expr_traffic = keyexpr::new("test/traffic").unwrap();
    let recv_bytes = AtomicU64::new(0);
    let start_time = Arc::new(RwLock::new(Instant::now()));
    let one_sec = Duration::from_secs(1);

    let _traffic_sub = session
        .declare_subscriber(key_expr_traffic)
        .callback(move |sample| {
            let mut start_time = start_time.write().unwrap();
            if start_time.elapsed() > one_sec {
                println!("Received traffic: {:.2} Mb/s", recv_bytes.swap(0, std::sync::atomic::Ordering::Relaxed) * 8 / 1000000);
                *start_time = Instant::now();
            }
            recv_bytes.fetch_add(sample.value.payload.len() as u64, std::sync::atomic::Ordering::Relaxed);
        })
        .res()
        .unwrap();

    let data: Value = (0usize..size)
        .map(|i| (i % 10) as u8)
        .collect::<Vec<u8>>()
        .into();

    let mut samples = Vec::with_capacity(n);

    // -- warmup --
    println!("Warming up for {warmup:?}...");
    let now = Instant::now();
    while now.elapsed() < warmup {
        let data = data.clone();
        publisher.put(data).res().unwrap();

        let _ = sub.recv();
    }

    for _ in 0..n {
        let data = data.clone();
        let write_time = Instant::now();
        publisher.put(data).res().unwrap();

        let sample = sub.recv();
        let ts = write_time.elapsed().as_micros();
        let len = sample.unwrap().value.payload.len();
        samples.push((ts, len));
    }

    let count = samples.len();
    let rtt_sum: u128 = samples.iter().map(|(ts, _)| ts).sum();
    let rtt_mean = rtt_sum / count as u128;


    let len_sum: u128 = samples.iter().map(|(_, len)| *len as u128).sum();
    let len_mean = len_sum / count as u128;

    println!(
        "{} bytes: rtt={:?}µs lat={:?}µs",
        len_mean,
        rtt_mean,
        rtt_mean / 2
    );

}

#[derive(Parser)]
struct Args {
    #[arg(short, long, default_value = "1")]
    /// The number of seconds to warm up (float)
    warmup: f64,
    #[arg(short = 'n', long, default_value = "100")]
    /// The number of round-trips to measure
    samples: usize,
    /// Sets the size of the payload to publish
    payload_size: usize,
    #[command(flatten)]
    common: CommonArgs,
}

fn parse_args() -> (Config, Duration, usize, usize) {
    let args = Args::parse();
    (
        args.common.into(),
        Duration::from_secs_f64(args.warmup),
        args.payload_size,
        args.samples,
    )
}
