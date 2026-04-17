#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT"

cargo run -p zenoh-examples --example z_repro_usrpwd_peer -- --name peer-a -c repro/usrpwd-duplicate-transport/peer-a.json5
