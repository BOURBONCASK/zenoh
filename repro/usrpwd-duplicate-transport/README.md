# Minimal Reproduction: Duplicate P2P Transport with `usrpwd`

This directory contains a minimal two-peer setup for reproducing the duplicate unicast transport issue triggered by `usrpwd` authentication.

## Topology

- `peer-a` listens on `tcp/127.0.0.1:18001`
- `peer-b` listens on `tcp/127.0.0.1:18002`
- both peers run in `peer` mode
- both peers explicitly connect to each other
- both peers enable `usrpwd`
- multicast scouting and gossip are disabled to keep the repro focused on duplicate transport establishment

## Why this setup is minimal

The original field report involved `peer + gossip + autoconnect`.

For transport-level debugging, that is more moving parts than necessary. This setup isolates the bug by forcing both peers to actively connect to each other. Once one side has already opened an outbound transport and the opposite direction arrives, the existing-transport reconciliation path is exercised directly.

## Files

- `peer-a.json5`
- `peer-b.json5`
- `usrpwd_dictionary.txt`
- `run-peer-a.sh`
- `run-peer-b.sh`

## How to run

Open two terminals from the repository root.

Terminal 1:

```bash
RUST_LOG=zenoh_transport::unicast::manager=trace,zenoh_transport::unicast::establishment::accept=trace,zenoh_transport::unicast::establishment::open=trace,zenoh::net::runtime::orchestrator=debug ./repro/usrpwd-duplicate-transport/run-peer-a.sh
```

Wait about one second, then in terminal 2:

```bash
RUST_LOG=zenoh_transport::unicast::manager=trace,zenoh_transport::unicast::establishment::accept=trace,zenoh_transport::unicast::establishment::open=trace,zenoh::net::runtime::orchestrator=debug ./repro/usrpwd-duplicate-transport/run-peer-b.sh
```

## Expected behavior before the fix

One of the peers should eventually emit logs similar to:

```text
Transport with peer ... already exist. Invalid config: ...
Received a close message (reason INVALID) in response to an OpenSyn
```

Under `usrpwd`, the typical mismatch is:

- existing outbound transport: `auth_id = None`
- duplicate inbound transport: `auth_id = Some("demo-user")`

## Expected behavior with the current workaround

This repro intentionally does **not** use `greater-zid`, because the goal is to preserve the duplicate connection condition that exposes the bug.
