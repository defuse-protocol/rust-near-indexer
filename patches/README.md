# Vendored crate patches

Crates here override their crates.io versions through `[patch.crates-io]` in
the root `Cargo.toml`. Each one is a temporary workaround for an upstream pin.
Delete it once upstream ships the fix.

## near-crypto 0.37.1

**Why:** RUSTSEC-2026-0382. `rustls-webpki` < 0.103.16 accepts certificate
chains through the leaked eMudhra (emSign) intermediate CAs. 0.103.16 requires
`aws-lc-rs ^1.18`, but nearcore 2.13.x (and therefore `near-crypto` 0.37.x)
pins `aws-lc-rs = "=1.16.2"` workspace-wide. We get near-crypto through
`blocksapi` → `near-indexer-primitives`, so nothing in this repo can bump it.

**Change:** a verbatim copy of the crates.io `near-crypto-0.37.1` source,
with two edits:
- `Cargo.toml`: `aws-lc-rs` `"=1.16.2"` → `"1.18"`.
- `src/signature.rs`: ML-DSA is now imported from `aws_lc_rs::signature`
  instead of `aws_lc_rs::unstable::signature`. aws-lc-rs 1.18 stabilized that
  API and left `#[deprecated]` aliases at the old path. Path dependencies
  don't get their warnings capped like registry crates do, so the old import
  printed 10 warnings in our build. The new line matches nearcore master.

The exact pin exists for node determinism. The indexer only deserializes
blocks and never relies on consensus-critical signature behavior, so
relaxing it is safe here.

**Remove when** a nearcore 2.13.x (or the release `blocksapi` moves to) ships
`aws-lc-rs >= 1.18`. nearcore master already pins `=1.18.1`. To remove: drop
the `[patch.crates-io]` entry, delete this directory, run `cargo update -p near-crypto`,
and confirm that `cargo tree -i rustls-webpki` still shows >= 0.103.16.
