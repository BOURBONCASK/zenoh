#!/usr/bin/env bash
#
# Runs the network-namespace tests of zenoh/tests/gossip_locator_freshness.rs.
#
# Those tests describe what has to happen when a node's routable address only
# appears after it bound its listeners. Reproducing that needs a namespace
# whose single interface has no IPv4 address until the test adds one, so they
# are #[ignore]d and a plain `cargo test` skips them; this script is how they
# are meant to be run.
#
# Each test gets a fresh namespace AND a fresh process: T1 asserts about what a
# process observes on its first interface lookup, so it cannot share a process
# with anything that already looked.
#
# Requirements: Linux, iproute2, and the ability to run `sudo unshare -n`
# (the test process itself must be root inside the namespace, because it
# configures the address at a precise point in the scenario).
#
# Usage:
#   ci/netns-check/run.sh [LOGDIR]
#
# Exits non-zero if any test fails; per-test logs land in LOGDIR when given.

set -u

SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" &>/dev/null && pwd)
WORKSPACE_DIR=$(cd -- "$SCRIPT_DIR/../.." &>/dev/null && pwd)
LOGDIR="${1:-}"

TEST_TARGET=gossip_locator_freshness
FEATURES="${FEATURES:-unstable internal}"

# Every #[ignore]d test in the target. Kept explicit rather than discovered so
# that a helper such as `child_peer_process`, which is re-executed by the tests
# themselves, is never started on its own.
NETNS_TESTS=(
    t1_enumeration_observes_address_added_after_first_touch
    t2_peer_bound_before_address_becomes_reachable
    t4_startup_storm_before_address_converges_after_address
    t5_control_isolated_peer_bound_after_address_is_reachable
    t5_isolated_peer_bound_before_address_becomes_reachable
)

if [ -n "$LOGDIR" ]; then
    mkdir -p "$LOGDIR"
fi

echo "Building $TEST_TARGET (features: $FEATURES)"
cargo test --manifest-path "$WORKSPACE_DIR/Cargo.toml" --no-run \
    -p zenoh --test "$TEST_TARGET" --features "$FEATURES" || exit 1

BIN=$(cargo test --manifest-path "$WORKSPACE_DIR/Cargo.toml" --no-run \
    -p zenoh --test "$TEST_TARGET" --features "$FEATURES" --message-format=json 2>/dev/null |
    sed -n 's/.*"executable":"\([^"]*'"$TEST_TARGET"'[^"]*\)".*/\1/p' | tail -1)

if [ -z "$BIN" ] || [ ! -x "$BIN" ]; then
    echo "could not locate the $TEST_TARGET test binary" >&2
    exit 1
fi
echo "Test binary: $BIN"

status=0
for t in "${NETNS_TESTS[@]}"; do
    echo "=== $t"
    # lo carries the router endpoint; dummy0 is the interface the test adds an
    # address to partway through. Neither has an IPv4 address at this point
    # beyond loopback's own.
    run() {
        sudo -n env "RUST_LOG=${RUST_LOG:-error}" unshare -n sh -c '
            set -e
            ip link set lo up
            ip link add dummy0 type dummy
            ip link set dummy0 up
            exec "$0" --exact "$1" --ignored --test-threads=1 --nocapture
        ' "$BIN" "$t"
    }
    if [ -n "$LOGDIR" ]; then
        run >"$LOGDIR/$t.log" 2>&1
        rc=$?
        echo "    exit=$rc (log: $LOGDIR/$t.log)"
    else
        run
        rc=$?
        echo "    exit=$rc"
    fi
    [ "$rc" -ne 0 ] && status=1
done

# T3 needs no namespace; run it here too so this script covers the whole file.
echo "=== connect_bookkeeping::t3_empty_locator_advertisement_does_not_block_later_dial"
if [ -n "$LOGDIR" ]; then
    "$BIN" --exact connect_bookkeeping::t3_empty_locator_advertisement_does_not_block_later_dial \
        --test-threads=1 --nocapture >"$LOGDIR/t3.log" 2>&1
    rc=$?
    echo "    exit=$rc (log: $LOGDIR/t3.log)"
else
    "$BIN" --exact connect_bookkeeping::t3_empty_locator_advertisement_does_not_block_later_dial \
        --test-threads=1 --nocapture
    rc=$?
    echo "    exit=$rc"
fi
[ "$rc" -ne 0 ] && status=1

exit "$status"
