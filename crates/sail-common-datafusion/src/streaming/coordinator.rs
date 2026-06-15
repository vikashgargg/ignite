//! `EpochCoordinator` — the **distributed barrier coordinator** for streaming exactly-once.
//!
//! This is Vajra's equivalent of Flink's *checkpoint coordinator* (JobManager) and RisingWave's
//! *meta service*: it owns the epoch clock for a distributed streaming job. It **triggers** epoch
//! barriers, collects an **ack** from every leaf task once that task has durably snapshotted its
//! slice of epoch `e` (source offsets + state-blob pointers), and when **all** expected tasks have
//! acked, declares epoch `e` **globally complete** — yielding a single [`GlobalCheckpoint`] the
//! driver writes atomically to the [`crate::streaming::checkpoint::CheckpointStore`] (one object =
//! no torn commit, the F4 principle). Recovery restores `last_committed` and every task restores its
//! `(op, partition, epoch)` state blob; sources seek to the committed offset.
//!
//! Design rules (grounded in Flink ABS + RisingWave): barriers/acks are per-epoch; acks are
//! **idempotent** (a task may resend); a checkpoint completes only when **every** task acks (the
//! Chandy-Lamport global-snapshot guarantee); committing epoch `e` **subsumes** all earlier in-flight
//! epochs (offsets/state advance monotonically), so `last_committed` is monotonic and stale/late acks
//! are ignored without error (common after recovery or abort). A bounded number of epochs may be
//! in flight at once (backpressure on the epoch clock). This type is pure, synchronous, and
//! unit-testable without a worker cluster — the I/O (durable commit record) is the driver's job.

use std::collections::{BTreeMap, BTreeSet};

use datafusion_common::{plan_err, Result};

/// Per-task acknowledgement for an epoch: the source offsets the task reached and pointers to the
/// state blobs it durably wrote. Merged across all tasks into the [`GlobalCheckpoint`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct EpochAck {
    /// `"topic:partition" -> next-offset` (the offset to resume from after this epoch).
    pub offsets: BTreeMap<String, i64>,
    /// `"op/partition" -> checkpoint-store key` of the state blob this task wrote for the epoch.
    pub state_ptrs: BTreeMap<String, String>,
}

/// The globally-consistent checkpoint emitted when every task has acked an epoch — the single record
/// the driver commits atomically (offsets + per-operator-instance state pointers).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GlobalCheckpoint {
    pub epoch: u64,
    pub offsets: BTreeMap<String, i64>,
    pub state_ptrs: BTreeMap<String, String>,
}

struct EpochState {
    acked: BTreeSet<u64>,
    merged: EpochAck,
}

/// Distributed barrier coordinator. Construct with the fixed set of leaf task ids that must ack each
/// epoch and the max number of concurrently in-flight epochs.
pub struct EpochCoordinator {
    expected: BTreeSet<u64>,
    in_flight: BTreeMap<u64, EpochState>,
    last_triggered: Option<u64>,
    last_committed: Option<u64>,
    max_in_flight: usize,
}

impl EpochCoordinator {
    /// `expected_tasks` is the set of leaf task ids that must ack each epoch (e.g. the sink tasks).
    /// `max_in_flight` bounds concurrently uncommitted epochs (backpressure on the epoch clock).
    pub fn new(expected_tasks: impl IntoIterator<Item = u64>, max_in_flight: usize) -> Result<Self> {
        let expected: BTreeSet<u64> = expected_tasks.into_iter().collect();
        if expected.is_empty() {
            return plan_err!("EpochCoordinator requires at least one expected task");
        }
        if max_in_flight == 0 {
            return plan_err!("EpochCoordinator requires max_in_flight >= 1");
        }
        Ok(Self {
            expected,
            in_flight: BTreeMap::new(),
            last_triggered: None,
            last_committed: None,
            max_in_flight,
        })
    }

    /// Begin the next epoch (sources start emitting `Checkpoint{epoch}`). Returns the new epoch id,
    /// or an error if `max_in_flight` uncommitted epochs are already outstanding (apply backpressure
    /// and retry later — never silently drop a trigger).
    pub fn trigger(&mut self) -> Result<u64> {
        if self.in_flight.len() >= self.max_in_flight {
            return plan_err!(
                "EpochCoordinator: {} epochs already in flight (max {})",
                self.in_flight.len(),
                self.max_in_flight
            );
        }
        let epoch = match (self.last_triggered, self.last_committed) {
            (Some(t), _) => t + 1,
            (None, Some(c)) => c + 1,
            (None, None) => 0,
        };
        self.last_triggered = Some(epoch);
        self.in_flight.insert(
            epoch,
            EpochState {
                acked: BTreeSet::new(),
                merged: EpochAck::default(),
            },
        );
        Ok(epoch)
    }

    /// Record `task`'s ack for `epoch`, merging its offsets/state pointers. Returns
    /// `Some(GlobalCheckpoint)` exactly when this ack completes the epoch (every expected task has
    /// acked) — at which point the epoch is committed (monotonic) and all in-flight epochs `<= epoch`
    /// are dropped (subsumed). Idempotent: a duplicate ack from the same task does not double-merge
    /// or re-complete. Stale/unknown acks (epoch already committed, aborted, or never triggered) are
    /// ignored and return `None` (expected after recovery — not an error).
    pub fn ack(
        &mut self,
        epoch: u64,
        task: u64,
        ack: EpochAck,
    ) -> Result<Option<GlobalCheckpoint>> {
        if !self.expected.contains(&task) {
            return plan_err!("EpochCoordinator: ack from unknown task {task}");
        }
        if self.last_committed.is_some_and(|c| epoch <= c) {
            return Ok(None); // stale ack for an already-committed (subsumed) epoch
        }
        let Some(state) = self.in_flight.get_mut(&epoch) else {
            return Ok(None); // unknown/aborted epoch — ignore
        };
        if state.acked.insert(task) {
            // First ack from this task for this epoch — merge its contribution.
            state.merged.offsets.extend(ack.offsets);
            state.merged.state_ptrs.extend(ack.state_ptrs);
        }
        if state.acked.len() < self.expected.len() {
            return Ok(None); // not all tasks have acked yet
        }
        // Globally complete: commit (monotonic) and drop all subsumed in-flight epochs.
        let merged = self.in_flight.remove(&epoch).map(|s| s.merged).unwrap_or_default();
        self.in_flight.retain(|&e, _| e > epoch);
        self.last_committed = Some(epoch);
        Ok(Some(GlobalCheckpoint {
            epoch,
            offsets: merged.offsets,
            state_ptrs: merged.state_ptrs,
        }))
    }

    /// Abort an in-flight epoch (a task failed it). The epoch is dropped and will be re-triggered;
    /// late acks for it are subsequently ignored. No-op if the epoch is not in flight.
    pub fn abort(&mut self, epoch: u64) {
        self.in_flight.remove(&epoch);
    }

    /// Restore coordinator state on recovery from the last globally-committed epoch (read from the
    /// durable global commit record). Clears any in-flight bookkeeping.
    pub fn recover(&mut self, last_committed: u64) {
        self.last_committed = Some(last_committed);
        self.last_triggered = Some(last_committed);
        self.in_flight.clear();
    }

    pub fn last_committed(&self) -> Option<u64> {
        self.last_committed
    }

    pub fn in_flight_count(&self) -> usize {
        self.in_flight.len()
    }
}

#[expect(clippy::unwrap_used)]
#[cfg(test)]
mod tests {
    use super::*;

    fn ack_with_offset(tp: &str, off: i64) -> EpochAck {
        let mut a = EpochAck::default();
        a.offsets.insert(tp.to_string(), off);
        a
    }

    #[test]
    fn completes_only_when_all_tasks_ack_and_merges_offsets() {
        let mut c = EpochCoordinator::new([1u64, 2, 3], 4).unwrap();
        let e = c.trigger().unwrap();
        assert_eq!(e, 0);
        assert!(c.ack(e, 1, ack_with_offset("t:0", 10)).unwrap().is_none());
        assert!(c.ack(e, 2, ack_with_offset("t:1", 20)).unwrap().is_none());
        let gc = c.ack(e, 3, ack_with_offset("t:2", 30)).unwrap().unwrap();
        assert_eq!(gc.epoch, 0);
        assert_eq!(gc.offsets.get("t:0"), Some(&10));
        assert_eq!(gc.offsets.get("t:1"), Some(&20));
        assert_eq!(gc.offsets.get("t:2"), Some(&30));
        assert_eq!(c.last_committed(), Some(0));
        assert_eq!(c.in_flight_count(), 0);
    }

    #[test]
    fn duplicate_ack_is_idempotent_and_does_not_complete_early() {
        let mut c = EpochCoordinator::new([1u64, 2], 4).unwrap();
        let e = c.trigger().unwrap();
        assert!(c.ack(e, 1, ack_with_offset("t:0", 5)).unwrap().is_none());
        // Same task acks again — must NOT complete the 2-task epoch.
        assert!(c.ack(e, 1, ack_with_offset("t:0", 999)).unwrap().is_none());
        let gc = c.ack(e, 2, ack_with_offset("t:1", 7)).unwrap().unwrap();
        // First ack wins (idempotent merge): offset stays 5, not the duplicate's 999.
        assert_eq!(gc.offsets.get("t:0"), Some(&5));
        assert_eq!(gc.offsets.get("t:1"), Some(&7));
    }

    #[test]
    fn unknown_task_ack_errors() {
        let mut c = EpochCoordinator::new([1u64], 4).unwrap();
        let e = c.trigger().unwrap();
        assert!(c.ack(e, 99, EpochAck::default()).is_err());
    }

    #[test]
    fn backpressure_caps_in_flight_epochs() {
        let mut c = EpochCoordinator::new([1u64], 2).unwrap();
        assert_eq!(c.trigger().unwrap(), 0);
        assert_eq!(c.trigger().unwrap(), 1);
        assert!(c.trigger().is_err()); // max_in_flight = 2 reached
        // Completing one frees a slot.
        c.ack(0, 1, EpochAck::default()).unwrap().unwrap();
        assert_eq!(c.trigger().unwrap(), 2);
    }

    #[test]
    fn stale_and_unknown_epoch_acks_are_ignored_not_errors() {
        let mut c = EpochCoordinator::new([1u64], 4).unwrap();
        let e = c.trigger().unwrap();
        c.ack(e, 1, EpochAck::default()).unwrap().unwrap(); // commit epoch 0
        // Late ack for the already-committed epoch 0 → ignored.
        assert!(c.ack(0, 1, EpochAck::default()).unwrap().is_none());
        // Ack for an epoch never triggered → ignored.
        assert!(c.ack(42, 1, EpochAck::default()).unwrap().is_none());
    }

    #[test]
    fn completing_later_epoch_subsumes_earlier_in_flight() {
        // Two epochs in flight; epoch 1 completes first → it commits and subsumes epoch 0, which is
        // dropped (its offsets are a subset). A late completion of epoch 0 is then ignored.
        let mut c = EpochCoordinator::new([1u64, 2], 4).unwrap();
        let e0 = c.trigger().unwrap();
        let e1 = c.trigger().unwrap();
        assert_eq!((e0, e1), (0, 1));
        c.ack(0, 1, ack_with_offset("t:0", 100)).unwrap(); // partial on epoch 0
        // Epoch 1 gets both acks → completes, subsumes epoch 0.
        assert!(c.ack(1, 1, ack_with_offset("t:0", 200)).unwrap().is_none());
        let gc = c.ack(1, 2, ack_with_offset("t:1", 200)).unwrap().unwrap();
        assert_eq!(gc.epoch, 1);
        assert_eq!(c.last_committed(), Some(1));
        assert_eq!(c.in_flight_count(), 0, "epoch 0 subsumed/dropped");
        // The straggler ack that would have completed epoch 0 is now ignored (stale).
        assert!(c.ack(0, 2, ack_with_offset("t:1", 100)).unwrap().is_none());
        assert_eq!(c.last_committed(), Some(1), "no regression to epoch 0");
    }

    #[test]
    fn recover_resumes_epoch_clock_after_committed() {
        let mut c = EpochCoordinator::new([1u64], 4).unwrap();
        c.recover(7);
        assert_eq!(c.last_committed(), Some(7));
        assert_eq!(c.trigger().unwrap(), 8, "next epoch continues after the committed one");
    }
}
