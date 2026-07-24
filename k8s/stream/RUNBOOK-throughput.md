# EKS throughput A/B + pinpoint runbook (decisive: beat Flink)

Goal: on the single 16-vCPU node, measure Zelox vs Flink 1.19 on the SAME 100M-event / 16-partition
topic (windowed COUNT), AND read the per-stage breakdown to pinpoint + fix the dominant stage.
Cost discipline: tear down to **$0** when done; mask 12-digit AWS account IDs in any pasted output.

## Pre-flight (cheap, do FIRST)
- [ ] Confirm the **i32 offset-overflow** is fixed at 100M (first EKS run hit it; verify the run reaches
      EndOfData, no `offset overflow` / i32 panic in the zelox-stream pod log).
- [ ] `eks-stream-cluster.yaml desiredCapacity: 1`, `zelox-stream replicas:1 --workers 4` (single node).
- [ ] Build + push the image with the latest `streaming/throughput-capstone` branch (has ZELOX_WM_PROF
      complete breakdown + error logging). `zelox-stream.yaml` already sets `ZELOX_WM_PROF=1`.

## Run
1. `kubectl apply -f k8s/stream/{eks-stream-cluster,kafka,zelox-stream,zelox-client}.yaml` (+ flink-session).
2. Producer: `kubectl apply -f k8s/stream/producer-job.yaml` (100M events, 16 partitions). Wait done.
3. **Zelox A/B:** from `zelox-client`, run `stream_windowed_agg.py` (availableNow). It prints
   `ZELOX_WAGG events=.. throughput=..M_events/s`. (Optional baseline: same run with `ZELOX_RT_MULTI`
   unset vs set — but the EKS path is bounded/availableNow, already 16-reader; RT_MULTI is continuous.)
4. **Flink:** `kubectl apply -f k8s/stream/flink-runner-job.yaml` (runs `flink-sql.sql`); read
   throughput from the Flink REST `/jobs` **job-duration** (excludes JVM/cluster startup — the honest
   compare).
5. **Pinpoint:** `kubectl logs deploy/zelox-stream -n stream | grep WM_PROF` → the per-stage line:
   `STAGES(summed-cpu-ms): source_read=.. from_json=.. exchange=.. encode=.. finalize=..`.
   **Rank the largest stage = the bottleneck to fix.** Errors (if any) appear as `KAFKA_SOURCE ...` /
   `STREAM_EXCHANGE ...` in the same log.

## Decide the fix from the breakdown (then implement once, prod-grade)
- `from_json` largest → **simd-json** parse (SIMD ≫ Flink Jackson; coordinate the dep w/ the
  version-upgrade repo). Confirmed-dominant before swapping the serde_json Value extraction.
- `exchange` largest → reduce per-batch copy/`concat`; arrow `Utf8View` (version upgrade) cuts shuffle
  copies; (multi-node → Arrow Flight).
- `source_read` largest → further rdkafka/builder tuning (already 2.1× once); check fetch sizes.
- `finalize` largest → DataFusion grouped-agg perf (version upgrade) / morsel parallelism.

## Teardown ($0)
`kubectl delete ns stream`; `eksctl delete cluster -f k8s/stream/eks-stream-cluster.yaml` (or scale
nodegroup to 0); verify no lingering NAT/EBS/EC2. Record the numbers in
`docs/design/throughput-robustness-review.md` + memory.
