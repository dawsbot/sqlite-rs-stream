use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rusqlite::Connection;
use sqlite_rs_stream::{Options, Replica, Replicator, RestoreOptions, restore};

fn app_conn(path: &Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.busy_timeout(Duration::from_secs(10)).unwrap();
    conn.query_row("PRAGMA journal_mode = wal", [], |r| r.get::<_, String>(0)).unwrap();
    conn
}

/// A digest of everything the test writes, for comparing databases.
fn digest(path: &Path) -> (i64, String) {
    let conn = Connection::open(path).unwrap();
    let ok: String = conn.query_row("PRAGMA integrity_check", [], |r| r.get(0)).unwrap();
    assert_eq!(ok, "ok", "integrity check failed for {}", path.display());
    conn.query_row(
        "SELECT count(*), coalesce(group_concat(id || ':' || length(body) || ':' || substr(body, 1, 8), ','), '')
         FROM (SELECT * FROM t ORDER BY id)",
        [],
        |r| Ok((r.get(0)?, r.get(1)?)),
    )
    .unwrap()
}

fn insert_batch(conn: &Connection, start: i64, n: i64) {
    let tx = conn.unchecked_transaction().unwrap();
    for i in start..start + n {
        tx.execute("INSERT INTO t (id, body) VALUES (?1, ?2)", (i, format!("{i:08}").repeat(1 + (i % 50) as usize)))
            .unwrap();
    }
    tx.commit().unwrap();
}

fn small_opts() -> Options {
    Options {
        sync_interval: Duration::from_millis(20),
        checkpoint_frames: 64,
        checkpoint_interval: Duration::from_millis(200),
        compaction_levels: vec![Duration::from_millis(100), Duration::from_millis(300)],
        snapshot_interval: Duration::from_millis(700),
        snapshot_retention: Duration::from_millis(500),
        l0_retention: Duration::from_millis(50),
    }
}

async fn restore_to(replica: &Replica, out: &Path, txid: Option<u64>) {
    restore(replica, out, &RestoreOptions { txid, overwrite: true }).await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn manual_sync_checkpoint_and_restore() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let conn = app_conn(&db);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)").unwrap();

    let replica = Replica::from_url(dir.path().join("replica").to_str().unwrap()).unwrap();
    let r = Replicator::open(&db, replica.clone(), Options::default()).await.unwrap();

    let first = r.sync().await.unwrap().unwrap();
    assert!(first.snapshot);
    assert_eq!(first.txid, 1);
    assert!(r.sync().await.unwrap().is_none(), "no changes means no new file");

    let mut txids = vec![];
    for round in 0..6 {
        insert_batch(&conn, round * 200, 200);
        let res = r.sync().await.unwrap().unwrap();
        assert!(!res.snapshot, "round {round} should be incremental");
        txids.push((res.txid, digest(&db)));
        if round % 2 == 1 {
            r.checkpoint().await.unwrap();
        }
    }
    // Shrinking the database must truncate it on restore too.
    conn.execute_batch("DELETE FROM t WHERE id % 3 = 0; VACUUM;").unwrap();
    let res = r.sync().await.unwrap().unwrap();
    txids.push((res.txid, digest(&db)));

    let stats = r.stats();
    assert_eq!(stats.snapshots, 1, "checkpoints must not force extra snapshots: {stats:?}");
    assert_eq!(stats.wal_restarts, 3, "{stats:?}");

    let out = dir.path().join("restored.db");
    restore_to(&replica, &out, None).await;
    assert_eq!(digest(&out), digest(&db));

    // Point-in-time restore to every TXID we recorded.
    for (txid, expected) in &txids {
        restore_to(&replica, &out, Some(*txid)).await;
        assert_eq!(&digest(&out), expected, "restore to txid {txid}");
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn background_replication_with_concurrent_writer() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_test_writer()
        .try_init();
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    app_conn(&db).execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)").unwrap();

    let replica = Replica::from_url(dir.path().join("replica").to_str().unwrap()).unwrap();
    let r = Arc::new(Replicator::open(&db, replica.clone(), small_opts()).await.unwrap());
    let handle = r.clone().start();

    // A writer with SQLite's default autocheckpoint, as an app would have.
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let db = db.clone();
        let stop = stop.clone();
        std::thread::spawn(move || {
            let conn = app_conn(&db);
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                insert_batch(&conn, i, 25);
                if i % 500 == 0 {
                    conn.execute("UPDATE t SET body = body || 'x' WHERE id % 7 = 0", []).unwrap();
                }
                i += 25;
                std::thread::sleep(Duration::from_millis(2));
            }
            i
        })
    };
    tokio::time::sleep(Duration::from_secs(3)).await;
    stop.store(true, Ordering::Relaxed);
    let rows = writer.join().unwrap();
    handle.stop().await.unwrap();

    let stats = r.stats();
    assert!(stats.syncs > 20, "expected many syncs, got {stats:?}");
    assert_eq!(stats.snapshots, 1, "only the initial sync should be a snapshot: {stats:?}");
    assert!(stats.checkpoints > 3, "expected checkpoints: {stats:?}");
    assert!(stats.wal_restarts > 3, "expected WAL restarts to be followed: {stats:?}");
    eprintln!("{stats:?}");

    let files = replica.list_all().await.unwrap();
    // Compaction, snapshots and retention should have pruned most of the L0 files.
    assert!(files.iter().any(|f| f.level == 9), "a snapshot expected: {files:?}");
    assert!((files.len() as u64) < stats.syncs / 4, "retention should prune: {} files for {stats:?}", files.len());

    let out = dir.path().join("restored.db");
    restore_to(&replica, &out, None).await;
    let (count, _) = digest(&out);
    assert_eq!(count, rows);
    assert_eq!(digest(&out), digest(&db));
}

#[tokio::test(flavor = "multi_thread")]
async fn reopen_continues_txids_with_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let conn = app_conn(&db);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)").unwrap();
    let replica = Replica::from_url(dir.path().join("replica").to_str().unwrap()).unwrap();

    let r = Replicator::open(&db, replica.clone(), Options::default()).await.unwrap();
    insert_batch(&conn, 0, 100);
    r.sync().await.unwrap();
    insert_batch(&conn, 100, 100);
    let last = r.sync().await.unwrap().unwrap().txid;
    drop(r);

    // Writes while no replicator is running, including a checkpoint.
    insert_batch(&conn, 200, 100);
    conn.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| Ok(())).unwrap();
    insert_batch(&conn, 300, 100);

    let r = Replicator::open(&db, replica.clone(), Options::default()).await.unwrap();
    let res = r.sync().await.unwrap().unwrap();
    assert!(res.snapshot);
    assert_eq!(res.txid, last + 1);

    let out = dir.path().join("restored.db");
    restore_to(&replica, &out, None).await;
    assert_eq!(digest(&out), digest(&db));
}

#[tokio::test(flavor = "multi_thread")]
async fn snapshot_compaction_and_retention_keep_restore_working() {
    let dir = tempfile::tempdir().unwrap();
    let db = dir.path().join("app.db");
    let conn = app_conn(&db);
    conn.execute_batch("CREATE TABLE t (id INTEGER PRIMARY KEY, body TEXT)").unwrap();
    let replica = Replica::from_url("memory://").unwrap();
    let opts = Options { l0_retention: Duration::ZERO, snapshot_retention: Duration::ZERO, ..Options::default() };
    let r = Replicator::open(&db, replica.clone(), opts).await.unwrap();

    for round in 0..5 {
        insert_batch(&conn, round * 100, 100);
        r.sync().await.unwrap();
    }
    assert!(r.compact(1).await.unwrap().is_some());
    assert!(r.compact(1).await.unwrap().is_none(), "nothing new to compact");
    r.snapshot().await.unwrap().unwrap();
    insert_batch(&conn, 500, 100);
    r.sync().await.unwrap();
    r.snapshot().await.unwrap().unwrap();
    insert_batch(&conn, 600, 100);
    r.sync().await.unwrap();

    let removed = r.enforce_retention().await.unwrap();
    assert!(removed > 0);
    let files = replica.list_all().await.unwrap();
    assert_eq!(files.iter().filter(|f| f.level == 9).count(), 1, "only newest snapshot kept: {files:?}");

    let out = dir.path().join("restored.db");
    restore_to(&replica, &out, None).await;
    assert_eq!(digest(&out), digest(&db));
}
