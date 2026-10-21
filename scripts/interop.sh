#!/usr/bin/env bash
# Cross-checks replicas against the Go reference implementations.
#
#   LITESTREAM=/path/to/litestream LTX=/path/to/ltx scripts/interop.sh
#
# Build both from source with:
#   git clone https://github.com/benbjohnson/litestream && (cd litestream && go build ./cmd/litestream)
#   git clone https://github.com/superfly/ltx && (cd ltx && go build ./cmd/ltx)
set -euo pipefail

LITESTREAM=${LITESTREAM:-litestream}
LTX=${LTX:-ltx}
cargo build --release --quiet
OURS=$(pwd)/target/release/sqlite-rs-stream

WORK=$(mktemp -d)
trap 'kill $(jobs -p) 2>/dev/null || true; rm -rf "$WORK"' EXIT

fingerprint() {
  sqlite3 "$1" "PRAGMA integrity_check;" | rg -qx ok || { echo "integrity check failed: $1" >&2; exit 1; }
  echo "$(sqlite3 "$1" "SELECT count(*) FROM t;") rows, sha $(sqlite3 "$1" "SELECT * FROM t ORDER BY id;" | shasum | cut -c1-12)"
}

write_rows() { # db first count
  for ((i = $2; i < $2 + $3; i += 100)); do
    sqlite3 "$1" "PRAGMA busy_timeout=5000; BEGIN; WITH RECURSIVE n(x) AS (SELECT $i UNION ALL SELECT x+1 FROM n WHERE x < $i + 99)
      INSERT INTO t (id, body) SELECT x, hex(randomblob(50 + x % 200)) FROM n; COMMIT;" >/dev/null
  done
}

stop() { kill -INT "$1"; wait "$1" 2>/dev/null || true; }

pass() { echo "PASS: $*"; }

# 1. sqlite-rs-stream replicates -> litestream restores, ltx verifies.
mkdir -p "$WORK/a"
sqlite3 "$WORK/a/app.db" "PRAGMA journal_mode=wal; CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT);" >/dev/null
"$OURS" replicate "$WORK/a/app.db" "$WORK/a/replica" --sync-interval 100ms 2>"$WORK/a/log" &
PID=$!
sleep 0.5
write_rows "$WORK/a/app.db" 0 5000
sleep 1
write_rows "$WORK/a/app.db" 5000 5000
sleep 1
stop $PID

"$LTX" verify "$WORK"/a/replica/ltx/*/*.ltx >/dev/null
pass "ltx verify accepts $(ls "$WORK"/a/replica/ltx/*/*.ltx | wc -l | tr -d ' ') files written by sqlite-rs-stream"

"$LITESTREAM" restore -o "$WORK/a/by-litestream.db" "file://$WORK/a/replica" 2>/dev/null
[[ "$(fingerprint "$WORK/a/app.db")" == "$(fingerprint "$WORK/a/by-litestream.db")" ]]
pass "litestream restores a sqlite-rs-stream replica: $(fingerprint "$WORK/a/by-litestream.db")"

# 2. litestream replicates -> sqlite-rs-stream restores.
mkdir -p "$WORK/b"
sqlite3 "$WORK/b/app.db" "PRAGMA journal_mode=wal; CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT);" >/dev/null
"$LITESTREAM" replicate "$WORK/b/app.db" "file://$WORK/b/replica" 2>"$WORK/b/log" &
PID=$!
sleep 1
write_rows "$WORK/b/app.db" 0 5000
sleep 2
write_rows "$WORK/b/app.db" 5000 5000
sleep 3
stop $PID

"$OURS" restore "$WORK/b/replica" -o "$WORK/b/by-ours.db" >/dev/null
[[ "$(fingerprint "$WORK/b/app.db")" == "$(fingerprint "$WORK/b/by-ours.db")" ]]
pass "sqlite-rs-stream restores a litestream replica: $(fingerprint "$WORK/b/by-ours.db")"

# 3. Hand-off: litestream starts a replica, sqlite-rs-stream continues it,
#    and litestream restores the combined history.
"$OURS" replicate "$WORK/b/app.db" "$WORK/b/replica" --sync-interval 100ms 2>"$WORK/b/log2" &
PID=$!
sleep 0.5
write_rows "$WORK/b/app.db" 10000 3000
sleep 1
stop $PID
"$LITESTREAM" restore -o "$WORK/b/handoff.db" "file://$WORK/b/replica" 2>/dev/null
[[ "$(fingerprint "$WORK/b/app.db")" == "$(fingerprint "$WORK/b/handoff.db")" ]]
pass "litestream restores a replica continued by sqlite-rs-stream: $(fingerprint "$WORK/b/handoff.db")"

# 4. Optional: the same round trip over S3. Litestream's S3 backend uses a
#    different key layout (<path>/<level %04x>/...) than its file backend.
#    INTEROP_S3_ENDPOINT=http://127.0.0.1:9000 INTEROP_S3_BUCKET=test \
#    AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=... scripts/interop.sh
if [[ -n "${INTEROP_S3_ENDPOINT:-}" ]]; then
  export AWS_ENDPOINT=$INTEROP_S3_ENDPOINT AWS_ALLOW_HTTP=true AWS_REGION=${AWS_REGION:-us-east-1}
  export LITESTREAM_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID LITESTREAM_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY
  PREFIX="interop-$$"
  Q="endpoint=$INTEROP_S3_ENDPOINT&region=$AWS_REGION&forcePathStyle=true"
  mkdir -p "$WORK/s3"
  sqlite3 "$WORK/s3/app.db" "PRAGMA journal_mode=wal; CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT);" >/dev/null
  "$OURS" replicate "$WORK/s3/app.db" "s3://$INTEROP_S3_BUCKET/$PREFIX" --sync-interval 100ms --lease 2>"$WORK/s3/log" &
  PID=$!
  sleep 0.5
  write_rows "$WORK/s3/app.db" 0 5000
  sleep 1
  stop $PID
  "$LITESTREAM" restore -o "$WORK/s3/by-litestream.db" "s3://$INTEROP_S3_BUCKET/$PREFIX?$Q" 2>/dev/null
  [[ "$(fingerprint "$WORK/s3/app.db")" == "$(fingerprint "$WORK/s3/by-litestream.db")" ]]
  pass "litestream restores a sqlite-rs-stream S3 replica: $(fingerprint "$WORK/s3/by-litestream.db")"
fi
