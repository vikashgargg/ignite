#!/usr/bin/env python3
"""Prove Zelox's windowed-agg output is IDENTICAL to Flink's — the "same S3/sink files as Flink" check.

Zelox output: parquet under a local dir (aws s3 cp'd), columns [window{start,end}, k, count].
Flink output:  JSONL (consumed from Kafka wagg_out), rows {"window_start","k","cnt"}.
Both are normalized to {(window_start_epoch_seconds, k): count} and compared for exact set + value equality.
Usage: compare_zelox_flink_output.py <zelox_parquet_dir> <flink_jsonl>
"""
import sys, json, glob, datetime
import pyarrow.parquet as pq
import pyarrow as pa

zelox_dir, flink_jsonl = sys.argv[1], sys.argv[2]

# --- Zelox ---
files = [f for f in glob.glob(f"{zelox_dir}/**/*.parquet", recursive=True) if "_spark_metadata" not in f]
vt = pa.concat_tables([pq.read_table(f) for f in files])
vt = vt.filter(pa.compute.greater_equal(vt.column("k"), 0))
vaj = {}
wstart = pa.compute.struct_field(vt.column("window"), "start")
ks = vt.column("k").to_pylist()
cs = vt.column("count").to_pylist()
for w, k, c in zip(wstart.to_pylist(), ks, cs):
    # w is a datetime (tz-aware or naive) -> epoch seconds
    ep = int(w.timestamp()) if isinstance(w, datetime.datetime) else int(w) // 1_000_000
    vaj[(ep, k)] = c

# --- Flink ---
flk = {}
for line in open(flink_jsonl):
    line = line.strip()
    if not line:
        continue
    r = json.loads(line)
    ws = r.get("window_start")
    # Flink TIMESTAMP(3) -> "2023-11-14 22:13:20" or ISO; parse to epoch seconds
    ep = None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"):
        try:
            ep = int(datetime.datetime.strptime(ws, fmt).replace(tzinfo=datetime.timezone.utc).timestamp())
            break
        except (ValueError, TypeError):
            continue
    if ep is None:
        try:
            ep = int(float(ws)) // (1000 if float(ws) > 1e12 else 1)
        except (ValueError, TypeError):
            continue
    flk[(ep, r.get("k"))] = r.get("cnt")

# --- Compare ---
vk, fk = set(vaj), set(flk)
only_v, only_f = vk - fk, fk - vk
mismatch = {k: (vaj[k], flk[k]) for k in (vk & fk) if vaj[k] != flk[k]}
identical = (not only_v) and (not only_f) and (not mismatch)
print(f"ZELOX groups={len(vaj)} sum={sum(vaj.values())} windows={len(set(k[0] for k in vaj))}")
print(f"FLINK groups={len(flk)} sum={sum(flk.values())} windows={len(set(k[0] for k in flk))}")
print(f"only_in_zelox={len(only_v)} only_in_flink={len(only_f)} value_mismatches={len(mismatch)}")
if mismatch:
    print("  sample mismatch:", list(mismatch.items())[:3])
print(f"OUTPUT_IDENTICAL={'YES' if identical else 'NO'}")
