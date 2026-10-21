# sqlite-rs-stream

Litestream-compatible SQLite replication as a Rust library. It runs inside your
process: no sidecar binary to install, configure, or keep versioned with each deploy.

It streams your database's WAL into [LTX](https://github.com/superfly/ltx) files on
S3 (or any S3-compatible store, or a local directory) and restores from them. The
files and key layout match Litestream v0.5+, so a replica written by this crate
can be restored by `litestream`, and the other way round.

> This is a disaster-recovery tool, not a distributed database. A standby only
> has data up to the last sync. There is no failover or merging.

## Use it as a library

```rust
use std::sync::Arc;
use sqlite_rs_stream::{Options, Replica, Replicator};

let replica = Replica::from_url("s3://my-bucket/app")?;
let replicator = Arc::new(Replicator::open("app.db", replica, Options::default()).await?);
let handle = replicator.clone().start(); // sync every 1s, checkpoint, compact, snapshot, prune

// ... use app.db with your own rusqlite connections ...

replicator.sync().await?; // optional: ship pending writes now (e.g. before acking a request)
handle.stop().await?;     // one final sync
```

```rust
use sqlite_rs_stream::{restore, Replica, RestoreOptions};

let replica = Replica::from_url("s3://my-bucket/app")?;
restore(&replica, "restored.db".as_ref(), &RestoreOptions::default()).await?;
// or a point in time: RestoreOptions { txid: Some(0x2a), ..Default::default() }
```

## Use it as a CLI

```sh
cargo install --path .
sqlite-rs-stream replicate app.db s3://my-bucket/app --lease
sqlite-rs-stream restore s3://my-bucket/app -o restored.db [--txid 000000000000002a]
sqlite-rs-stream files s3://my-bucket/app
sqlite-rs-stream verify some-file.ltx
```

Replica URLs:
- `/path/to/dir` or `file:///path/to/dir`
- `s3://bucket/prefix`. Configure it with the standard `AWS_*` variables:
  `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION`, and for MinIO, R2 or
  Tigris also `AWS_ENDPOINT` (plus `AWS_ALLOW_HTTP=true` for plain HTTP).
- `memory://` (for tests)

## How it works

| Piece | What it does |
|---|---|
| `wal` | Parses the WAL header and frames, follows the checksum chain, and collects pages from committed transactions only. |
| `ltx` | Streaming LTX encoder/decoder: LZ4 block-compressed pages, varint page index, CRC-64/ISO file checksum. Also k-way compaction. |
| `db::Replicator` | Holds a read transaction so SQLite cannot reset the WAL under it, and ships new frames as level-0 LTX files. It also runs the checkpoints itself. |
| `replica` | `object_store` backend and Litestream's key layout: `<prefix>/<level %04x>/` on S3, `ltx/<level>/` for directories. Also builds the restore plan. |
| `restore` | Downloads the plan one file at a time and writes pages straight into the output file, so memory stays bounded by a page. |
| `lease` | Litestream's `lock.json` lease, using conditional PUTs (`If-None-Match` / `If-Match`), so only one host replicates at a time. |

Checkpointing works like this:
1. Take the write lock on a second connection.
2. Only then release the read transaction.
3. Ship the WAL tail.
4. Run a `PASSIVE` checkpoint.
5. Release the write lock and start a new read transaction.

The next writer restarts the WAL, which bumps salt-1 by exactly one, and the
replicator follows that incrementally. Any WAL change it can't account for makes
the next level-0 file a full snapshot, which is always safe to apply.

Compaction levels default to Litestream's: L1 every 30s, L2 every 5m, L3 every
1h, and an L9 snapshot daily. Retention prunes files once a higher level covers them.

## Verified

- `cargo test`: LTX round trips, corruption detection, compaction semantics, the
  restore plan, and leasing. Integration tests cover a concurrent writer with
  checkpoints and WAL restarts, point-in-time restore of every TXID, VACUUM
  shrink, reopening, and retention.
- `scripts/interop.sh` against the Go reference binaries:
  - Go `ltx verify` accepts every file this crate writes.
  - `litestream restore` restores a replica written by this crate.
  - This crate restores a replica written by `litestream replicate`.
  - A replica started by Litestream and continued by this crate restores with Litestream.
  - Optionally, the same round trip over S3 (tested with `rclone serve s3`).
- `cargo run --release --example bench -- 100000` on an M-series Mac, local
  directory replica:

  | rows | WAL | sync() | restore() | peak RSS |
  |---|---|---|---|---|
  | 100k | 2.2 MB | ~12–16 ms | ~20–24 ms | 18.7 MB |
  | 400k | 9.2 MB | ~34 ms | ~45 ms | 23.7 MB |

## Limitations (MVP)

- **One full snapshot per process start.** The replication position is not
  persisted locally, so the first sync after `open` ships the whole database as
  an L0 file. TXIDs still continue from the replica.
- **Use the same SQLite library as your app.** With two copies of SQLite in one
  process, POSIX locks can corrupt the database. If your app uses `rusqlite`,
  Cargo unifies on one `libsqlite3-sys`. The `bundled` feature is on by default.
- **The app must not run `wal_checkpoint(RESTART|TRUNCATE)` itself.** It would
  block on the replicator's read lock. Default autocheckpoints are fine.
- **Leasing needs conditional writes.** S3 and most S3-compatible stores
  support them; local directories don't.
- Not implemented yet: restore by timestamp, GCS/Azure/SFTP backends, legacy
  (pre-0.5) LZ4-frame LTX pages, and Litestream v0.3 generation-format replicas.
- Not yet tested against real AWS S3 or under crash and fault injection.

## Credit

This is a port of [Litestream](https://github.com/benbjohnson/litestream) by Ben
Johnson and of [superfly/ltx](https://github.com/superfly/ltx). The file format,
key layout and replication approach are theirs.
