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
use std::io::{stdin, Read};
use std::thread::sleep;
use std::time::Duration;
use clap::Parser;
use zenoh::config::Config;
use zenoh::prelude::sync::*;
use zenoh::publication::CongestionControl;
use zenoh_examples::CommonArgs;

fn main() {
    // initiate logging
    env_logger::init();

    let args = Args::parse();
    let config: Config = args.common.into();

    let session = zenoh::open(config).res().unwrap().into_arc();

    // The key expression to read the data from
    let key_expr_ping = keyexpr::new("test/ping").unwrap();

    // The key expression to echo the data back
    let key_expr_pong = keyexpr::new("test/pong").unwrap();

    let publisher = session
        .declare_publisher(key_expr_pong)
        .congestion_control(CongestionControl::Block)
        .res()
        .unwrap();

    let _sub = session
        .declare_subscriber(key_expr_ping)
        .callback(move |sample| publisher.put(sample.value).res().unwrap())
        .res()
        .unwrap();

    if args.traffic_size > 0 {
        // The key expression to send parallel traffic
        let key_expr_traffic = keyexpr::new("test/traffic").unwrap();

        let traffic_publisher = session
            .declare_publisher(key_expr_traffic)
            .congestion_control(CongestionControl::Drop)
            .res()
            .unwrap();

        let traffic_data: Value = (0..args.traffic_size)
            .map(|i| (i % 10) as u8)
            .collect::<Vec<u8>>()
            .into();
        let traffic_period = Duration::from_secs_f64(1_f64 / args.traffic_freq as f64);
        println!("Publishing parallel traffic of {} bytes at {}ms interval => {:.2} Mb/s",
            args.traffic_size,
            traffic_period.as_millis(),
            (args.traffic_size * 8 * args.traffic_freq) as f64 / 1000000_f64
        );
        loop {
            traffic_publisher.put(traffic_data.clone()).res().unwrap();
            sleep(traffic_period);
        }
    } else {
        for _ in stdin().bytes().take_while(|b| !matches!(b, Ok(b'q'))) {}
    }

}

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(short = 's', long, default_value = "400000")]
    traffic_size: usize,
    #[arg(short = 'f', long, default_value = "30")]
    traffic_freq: usize,
}
