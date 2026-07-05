#!/usr/bin/env bash
# Disk-pressure watchdog for the Vajra build tree — PROD-GRADE, threshold-driven.
#
# Why this exists: a once-a-day cron cannot react to a build loop that fills tens of GiB in
# hours. This script is meant to run FREQUENTLY (every ~10 min via launchd, see
# scripts/install_prune_watchdog.sh) and act on ACTUAL free-disk pressure, not the clock.
#
# Tiers (least → most destructive), gated on free space:
#   • ALWAYS (any run): prune docker dangling images + builder cache (never touches the build).
#   • WARN  (< WARN_GB): drop `target/*/incremental` — safe, cargo regenerates it. With
#     dev/test `incremental = false` (Cargo.toml) this is normally empty; kept as a belt-and-
#     suspenders for older artifacts / other checkouts.
#   • CRIT  (< CRIT_GB) AND no compile running: full `cargo clean` + `docker system prune`.
#     If a compile IS running we DO NOT cargo-clean (would corrupt it) — we log LOUDLY so the
#     signal isn't silently swallowed (the old script's fatal flaw: it skipped entirely while a
#     build ran, i.e. exactly when the disk was filling).
#
# Idempotent, safe to run concurrently-ish (best-effort flock), self-rotating log.
set -uo pipefail

ROOT="${VAJRA_ROOT:-/Users/vikashgarg/Desktop/ignite}"
WARN_GB="${VAJRA_PRUNE_WARN_GB:-30}"   # below this: drop incremental caches
CRIT_GB="${VAJRA_PRUNE_CRIT_GB:-15}"   # below this: full cargo clean (only if no build running)
LOG="${VAJRA_PRUNE_LOG:-/tmp/vajra_prune.log}"
LOCK="/tmp/vajra_prune.lock"

cd "$ROOT" 2>/dev/null || exit 0

# Best-effort single-instance guard.
exec 9>"$LOCK" 2>/dev/null || true
if command -v flock >/dev/null 2>&1; then flock -n 9 || exit 0; fi

log() { printf '%s %s\n' "$(date '+%F %T')" "$*" >> "$LOG"; }

# Rotate log if it grows past ~1 MiB (keep one .1 backup).
if [ -f "$LOG" ] && [ "$(wc -c < "$LOG" 2>/dev/null || echo 0)" -gt 1048576 ]; then
  mv -f "$LOG" "$LOG.1" 2>/dev/null || true
fi

# Free space in whole GiB (macOS/BSD df: -k = 1024-blocks, portable + correct).
free_gb() { df -k "$ROOT" 2>/dev/null | awk 'NR==2 { printf "%d", $4/1024/1024 }'; }
build_running() { pgrep -x rustc >/dev/null 2>&1 || pgrep -x cargo >/dev/null 2>&1; }

FREE_BEFORE="$(free_gb)"; FREE_BEFORE="${FREE_BEFORE:-999}"

# Tier 0 — always safe, never touches the build tree. Prune dangling build cache + images every
# run (cheap). NOTE: this reclaims space *inside* the Docker Desktop VM; the VM's backing disk
# image does not auto-shrink, so macOS-visible free space only returns on newer Docker Desktop.
# No-op (via `|| true`) when the daemon is down.
docker builder prune -f >/dev/null 2>&1 || true
docker image   prune -f >/dev/null 2>&1 || true
ACTION="tier0(docker)"

if [ "$FREE_BEFORE" -lt "$WARN_GB" ]; then
  INCR="$(du -sh target/*/incremental 2>/dev/null | awk '{print $1}' | paste -sd, -)"
  rm -rf target/*/incremental 2>/dev/null || true
  # Under pressure, prune ALL unused docker (incl. tagged images + volumes), not just dangling.
  docker system prune -af --volumes >/dev/null 2>&1 || true
  ACTION="warn: dropped incremental(${INCR:-none}) + docker system prune -af"
fi

if [ "$FREE_BEFORE" -lt "$CRIT_GB" ]; then
  if build_running; then
    # CRITICAL but a compile is live: cannot cargo-clean safely. Log loudly; Tier-0/incremental
    # prune above is the most we can safely do mid-build.
    log "CRITICAL ${FREE_BEFORE}GB < ${CRIT_GB}GB but BUILD RUNNING — skipped cargo clean (unsafe); did incremental+docker only"
    ACTION="crit(build-running): incremental+docker only"
  else
    cargo clean >/dev/null 2>&1 || true
    docker system prune -f >/dev/null 2>&1 || true
    ACTION="crit: cargo clean + docker system prune"
  fi
fi

FREE_AFTER="$(free_gb)"; FREE_AFTER="${FREE_AFTER:-?}"
log "${ACTION}; free ${FREE_BEFORE}GB -> ${FREE_AFTER}GB (warn<${WARN_GB} crit<${CRIT_GB})"
