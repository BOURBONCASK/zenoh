//
// Copyright (c) 2026 Contributors
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//

use clap::Parser;
use zenoh_examples::CommonArgs;

#[tokio::main]
async fn main() {
    zenoh::init_log_from_env_or("info");

    let args = Args::parse();
    let config: zenoh::Config = args.common.into();

    println!("[{}] opening session...", args.name);
    let session = zenoh::open(config).await.expect("failed to open session");
    println!("[{}] zid={}", args.name, session.info().zid().await);
    println!("[{}] waiting for Ctrl-C", args.name);

    tokio::signal::ctrl_c()
        .await
        .expect("failed to wait for Ctrl-C");

    println!("[{}] shutting down", args.name);
}

#[derive(clap::Parser, Clone, PartialEq, Eq, Hash, Debug)]
struct Args {
    #[command(flatten)]
    common: CommonArgs,
    #[arg(long)]
    name: String,
}
