#!/usr/bin/env bash
# LOCAL repro of the realtime windowed-agg KEY-CORRUPTION bug (distinct_k < 1000). FREE, no cloud.
# vajra server (realtime) + docker Kafka + realtime windowed drain -> local parquet -> per-key verify.
# Matrix showed it triggers at 10M/w1; this sweeps scale/workers locally to find the smallest repro + trace.
# Usage: N=2000000 WORKERS=2 scripts/local_realtime_keycheck.sh
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"; cd "$ROOT"
N="${N:-2000000}"; WORKERS="${WORKERS:-2}"; KEYS="${KEYS:-1000}"; PORT="${PORT:-50099}"; NP="${NP:-4}"
TOPIC="rtkey_${N}_${WORKERS}"; OUT="/tmp/rtk_out_${N}_${WORKERS}"; CK="/tmp/rtk_ck_${N}_${WORKERS}"
BIN="$ROOT/target/debug/vajra"; PY="$ROOT/.venvs/smoke/bin/python"
BOOT="${BOOT:-localhost:9092}"
[ -x "$BIN" ] || { echo "FATAL: need $BIN (cargo build -p sail-cli --bin vajra)"; exit 2; }
KP=$(docker ps --format '{{.Names}}' | grep -i kafka | head -1); [ -n "$KP" ] || { echo "FATAL: no kafka container"; exit 2; }
rm -rf "$OUT" "$CK"
echo "=== [1] topic $TOPIC + produce N=$N keys=$KEYS (+closers) ==="
docker exec "$KP" /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --create --if-not-exists --topic "$TOPIC" --partitions "$NP" --replication-factor 1 >/dev/null 2>&1 || true
BOOT="$BOOT" TOPIC="$TOPIC" N="$N" K="$KEYS" EPMS=100 NP="$NP" CLOSER_TS=1700000200000 "$PY" scripts/scale_producer.py 2>&1 | tail -1 || true
echo "=== [2] vajra server (realtime, workers=$WORKERS) ==="
RUST_LOG="${RUST_LOG:-warn}" "$BIN" server --ip 127.0.0.1 --port "$PORT" --mode local-cluster --workers "$WORKERS" > "/tmp/rtk_server_${N}_${WORKERS}.log" 2>&1 &
SRV=$!; trap "kill $SRV 2>/dev/null" EXIT
for i in $(seq 1 40); do "$PY" -c "import socket;socket.create_connection(('127.0.0.1',$PORT),1).close()" 2>/dev/null && break; sleep 1; done
echo "=== [3] realtime windowed drain -> $OUT ==="
SPARK_REMOTE="sc://localhost:$PORT" BOOT="$BOOT" TOPIC="$TOPIC" N_EVENTS="$N" RT_DUR="2 seconds" MAX_SECS=200 \
  OUT="$OUT" CK="$CK" "$PY" scripts/stream_realtime_drain.py 2>&1 | grep -aiE "VAJRA_COMPLETENESS|VAJRA_REALTIME" | tail -2
echo "=== [4] PER-KEY verify (the check aggregate-blind gates missed) ==="
"$PY" - "$OUT" "$KEYS" <<'PY'
import sys,glob,os,collections
import pyarrow.parquet as pq, pyarrow.compute as pc
OUT,KEYS=sys.argv[1],int(sys.argv[2])
files=[f for f in glob.glob(f"{OUT}/**/*.parquet",recursive=True) if "_spark_metadata" not in f]
allk=set(); rows=0; g=collections.Counter()
for f in files:
    t=pq.read_table(f); t=t.filter(pc.greater_equal(t.column("k"),0)); rows+=t.num_rows
    ws=[x["start"] for x in t.column("window").to_pylist()]
    for w,k,c in zip(ws,t.column("k").to_pylist(),t.column("count").to_pylist()):
        allk.add(k); g[(w,k)]+=c
miss=len(set(range(KEYS))-allk)
print(f"LOCAL_KEYCHECK files={len(files)} rows={rows} distinct_k={len(allk)} missing={miss} "
      f"groups={len(g)} KEYS_OK={len(allk)==KEYS and miss==0}")
PY
