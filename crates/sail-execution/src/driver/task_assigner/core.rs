use std::collections::HashSet;

use indexmap::IndexSet;
use log::{error, warn};

use crate::driver::task_assigner::state::{TaskSlot, WorkerResource};
use crate::driver::task_assigner::{TaskAssigner, TaskRegion};
use crate::id::{JobId, TaskKey, WorkerId};
use crate::job_graph::TaskPlacement;
use crate::task::scheduling::{
    TaskAssignment, TaskAssignmentGetter, TaskSetAssignment, TaskStreamAssignment,
};

impl TaskAssigner {
    pub fn request_workers(&mut self) -> usize {
        let enqueued_slots = self
            .task_queue
            .iter()
            .map(|region| {
                region
                    .tasks
                    .iter()
                    .filter(|(placement, _)| matches!(placement, TaskPlacement::Worker))
                    .count()
            })
            .sum::<usize>();
        let vacant_slots = self
            .workers
            .values()
            .map(|worker| match worker {
                WorkerResource::Active { task_slots, .. } => {
                    task_slots.iter().filter(|x| x.is_vacant()).count()
                }
                WorkerResource::Inactive => 0,
            })
            .sum::<usize>();
        let required_slots = enqueued_slots.saturating_sub(vacant_slots);
        let active_workers = self
            .workers
            .values()
            .filter(|worker| matches!(worker, WorkerResource::Active { .. }))
            .count();
        let allowed_workers = if self.options.worker_max_count == 0 {
            usize::MAX
        } else {
            self.options
                .worker_max_count
                .saturating_sub(self.requested_worker_count)
                .saturating_sub(active_workers)
        };
        let required_workers = required_slots
            .div_ceil(self.options.worker_task_slots)
            .min(allowed_workers);
        self.requested_worker_count = self.requested_worker_count.saturating_add(required_workers);
        required_workers
    }

    pub fn track_worker_failed_to_start(&mut self) {
        self.requested_worker_count = self.requested_worker_count.saturating_sub(1);
    }

    pub fn activate_worker(&mut self, worker_id: WorkerId) {
        self.requested_worker_count = self.requested_worker_count.saturating_sub(1);
        if self.workers.contains_key(&worker_id) {
            warn!("worker {worker_id} is already active");
            return;
        }
        self.workers.insert(
            worker_id,
            WorkerResource::Active {
                task_slots: vec![TaskSlot::default(); self.options.worker_task_slots],
                local_streams: IndexSet::new(),
            },
        );
    }

    pub fn deactivate_worker(&mut self, worker_id: WorkerId) {
        let Some(worker) = self.workers.get_mut(&worker_id) else {
            warn!("worker {worker_id} not found");
            return;
        };
        match worker {
            WorkerResource::Active { .. } => {
                *worker = WorkerResource::Inactive;
            }
            WorkerResource::Inactive => {
                warn!("worker {worker_id} is already inactive");
            }
        }
    }

    pub fn enqueue_tasks(&mut self, region: TaskRegion) {
        self.task_queue.push_back(region);
    }

    pub fn exclude_task(&mut self, key: &TaskKey) {
        self.task_queue.retain(|x| !x.contains(key));
    }

    pub fn assign_tasks(&mut self) -> Vec<TaskSetAssignment> {
        let mut assignments = vec![];
        let mut assigner = self.build_worker_task_slot_assigner();

        while let Some(region) = self.task_queue.pop_front() {
            match assigner.try_assign_task_region(region) {
                Ok(x) => assignments.extend(x),
                Err(region) => {
                    // The region cannot be successfully assigned as a whole
                    // due to insufficient worker task slots.
                    // Put the region back to the queue and try again later.
                    // We must put the region back to the front of the queue to
                    // avoid starvation.
                    // This does result in head-of-line blocking, but we would
                    // like the regions to be assigned in the same order as they
                    // are enqueued.
                    self.task_queue.push_front(region);
                    break;
                }
            }
        }

        // Update the driver and worker based on the assignments.
        for assignment in assignments.iter() {
            match assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.add_task_set(assignment.set.clone());
                    for key in assignment.set.tasks() {
                        self.task_assignments
                            .insert(key.clone(), TaskAssignment::Driver);
                    }
                }
                TaskAssignment::Worker { worker_id, slot } => {
                    if let Some(worker) = self.workers.get_mut(&worker_id) {
                        worker.add_task_set(slot, assignment.set.clone());
                        for key in assignment.set.tasks() {
                            self.task_assignments
                                .insert(key.clone(), TaskAssignment::Worker { worker_id, slot });
                        }
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }

        assignments
    }

    pub fn unassign_task(&mut self, key: &TaskKey) -> Option<TaskAssignment> {
        let assignment = self.task_assignments.get(key)?;
        match assignment {
            TaskAssignment::Driver => {
                self.driver.remove_task(key);
            }
            TaskAssignment::Worker { worker_id, slot } => {
                let Some(worker) = self.workers.get_mut(worker_id) else {
                    warn!("worker {worker_id} not found");
                    return None;
                };
                worker.remove_task(key, *slot);
            }
        }
        Some(assignment.clone())
    }

    /// Records local and remote stream ownership for each resource based on the given task assignments.
    pub fn track_streams(&mut self, assignments: &[TaskSetAssignment]) {
        for assignment in assignments {
            self.driver.track_remote_streams(&assignment.set);
            match &assignment.assignment {
                TaskAssignment::Driver => {
                    self.driver.track_local_streams(&assignment.set);
                }
                TaskAssignment::Worker { worker_id, .. } => {
                    if let Some(worker) = self.workers.get_mut(worker_id) {
                        worker.track_local_streams(&assignment.set);
                    } else {
                        error!("worker {worker_id} not found");
                    }
                }
            }
        }
    }

    pub fn untrack_local_streams(
        &mut self,
        job_id: JobId,
        stage: Option<usize>,
    ) -> HashSet<TaskStreamAssignment> {
        let mut assignments = HashSet::new();
        if self.driver.untrack_local_streams(job_id, stage) {
            assignments.insert(TaskStreamAssignment::Driver);
        }
        for (worker_id, worker) in self.workers.iter_mut() {
            if matches!(worker, WorkerResource::Active { .. })
                && worker.untrack_local_streams(job_id, stage)
            {
                assignments.insert(TaskStreamAssignment::Worker {
                    worker_id: *worker_id,
                });
            }
        }
        assignments
    }

    pub fn untrack_remote_streams(&mut self, job_id: JobId, stage: Option<usize>) -> bool {
        self.driver.untrack_remote_streams(job_id, stage)
    }

    pub fn is_worker_idle(&self, worker_id: WorkerId) -> bool {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return false;
        };
        match worker {
            WorkerResource::Active {
                task_slots: slots,
                local_streams: streams,
            } => slots.iter().all(|s| s.is_vacant()) && streams.is_empty(),
            WorkerResource::Inactive => false,
        }
    }

    pub fn find_worker_tasks(&self, worker_id: WorkerId) -> Vec<TaskKey> {
        let Some(worker) = self.workers.get(&worker_id) else {
            warn!("worker {worker_id} not found");
            return vec![];
        };
        match worker {
            WorkerResource::Active {
                task_slots: slots, ..
            } => slots
                .iter()
                .flat_map(|x| x.list_tasks().cloned().collect::<Vec<_>>())
                .collect(),
            WorkerResource::Inactive => vec![],
        }
    }

    /// Builds a snapshot of available task slots across the driver and active workers for assignment.
    fn build_worker_task_slot_assigner(&self) -> TaskSlotAssigner {
        let slots = self
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                let slots = match worker {
                    WorkerResource::Inactive => vec![],
                    WorkerResource::Active {
                        task_slots: slots, ..
                    } => slots
                        .iter()
                        .enumerate()
                        .filter_map(|(i, s)| s.is_vacant().then_some(i))
                        .collect(),
                };
                if !slots.is_empty() {
                    Some((*id, slots))
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        TaskSlotAssigner::new(slots, placement_policy())
    }
}

impl TaskAssignmentGetter for TaskAssigner {
    fn get(&self, key: &TaskKey) -> Option<&TaskAssignment> {
        self.task_assignments.get(key)
    }
}

/// VAJ-BF2 T-BF2.5: task-placement policy across workers.
///
/// **Grounded (docs/REFERENCES.md — scheduler/placement):** Flink `cluster.evenly-spread-out-slots`
/// (SlotManager places each slot on the TaskManager with the MOST free slots so subtasks fan out
/// evenly across TMs) and Spark `spark.deploy.spreadOut` (spread executors/tasks across workers).
/// Flink's *default* is fill-first (resource-efficient for reactive scale-down); even-spread is the
/// opt-in that maximizes parallelism/utilization on a fixed cluster — which is what a distributed
/// streaming stage needs (a keyed windowed-agg's N instances must land on different workers, not one).
#[derive(Clone, Copy, PartialEq, Eq)]
enum PlacementPolicy {
    /// Fill one worker's slots before the next (Flink default; resource-efficient, but co-locates a
    /// stage's partitions on one worker — the T2-measured cause of "all 8 window instances on 1 pod").
    FillFirst,
    /// Place on the worker with the most free slots (Flink evenly-spread-out-slots) so a stage's
    /// partitions distribute across workers. Deterministic: ties break to the lowest slot-vector index.
    EvenSpread,
}

/// Resolve the placement policy ONCE (process-global config, like `channel_capacity`). Even-spread is
/// enabled when distributed streaming is on (`VAJRA_DISTRIBUTED_STREAM=1`) — a cut streaming stage only
/// actually distributes if its partitions also spread — or explicitly via `VAJRA_EVEN_SPREAD=1`. Default
/// stays FillFirst so batch/existing placement is unchanged (additive + reversible).
fn placement_policy() -> PlacementPolicy {
    static POLICY: std::sync::OnceLock<PlacementPolicy> = std::sync::OnceLock::new();
    *POLICY.get_or_init(|| {
        let on = std::env::var("VAJRA_DISTRIBUTED_STREAM").as_deref() == Ok("1")
            || std::env::var("VAJRA_EVEN_SPREAD").as_deref() == Ok("1");
        if on {
            PlacementPolicy::EvenSpread
        } else {
            PlacementPolicy::FillFirst
        }
    })
}

/// Assigns task regions to driver or worker slots, consuming available slots as tasks are placed.
struct TaskSlotAssigner {
    /// The available task slots on workers.
    slots: Vec<(WorkerId, Vec<usize>)>,
    policy: PlacementPolicy,
}

impl TaskSlotAssigner {
    fn new(slots: Vec<(WorkerId, Vec<usize>)>, policy: PlacementPolicy) -> Self {
        Self { slots, policy }
    }

    fn next(&mut self) -> Option<(WorkerId, usize)> {
        match self.policy {
            PlacementPolicy::FillFirst => self
                .slots
                .iter_mut()
                .find_map(|(worker_id, slots)| slots.pop().map(|slot| (*worker_id, slot))),
            PlacementPolicy::EvenSpread => {
                // Pick the worker with the most free slots (Flink evenly-spread-out-slots). Strict `>`
                // keeps the FIRST worker on ties (lowest index) → deterministic round-robin across
                // equal-capacity workers, so a stage's N partitions fan out one-per-worker.
                let mut best: Option<usize> = None;
                let mut best_len = 0usize;
                for (i, (_, slots)) in self.slots.iter().enumerate() {
                    if slots.len() > best_len {
                        best_len = slots.len();
                        best = Some(i);
                    }
                }
                let i = best?;
                let (worker_id, slots) = &mut self.slots[i];
                slots.pop().map(|slot| (*worker_id, slot))
            }
        }
    }

    fn try_assign_task_region(
        &mut self,
        region: TaskRegion,
    ) -> Result<Vec<TaskSetAssignment>, TaskRegion> {
        let mut assignments = vec![];

        for (placement, set) in &region.tasks {
            match placement {
                TaskPlacement::Driver => {
                    assignments.push(TaskSetAssignment {
                        set: set.clone(),
                        assignment: TaskAssignment::Driver,
                    });
                }
                TaskPlacement::Worker => {
                    if let Some((worker_id, slot)) = self.next() {
                        assignments.push(TaskSetAssignment {
                            set: set.clone(),
                            assignment: TaskAssignment::Worker { worker_id, slot },
                        });
                    } else {
                        // The worker task slots are not enough for assigning all the
                        // worker tasks in this region. So we return the region back
                        // to indicate the error.
                        return Err(region);
                    }
                }
            }
        }
        Ok(assignments)
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod placement_tests {
    use crate::id::WorkerId;

    use super::{PlacementPolicy, TaskSlotAssigner};

    // 3 workers, 2 vacant slots each. Slot indices per worker are irrelevant to the policy.
    fn three_workers() -> Vec<(WorkerId, Vec<usize>)> {
        vec![
            (WorkerId::from(1), vec![0, 1]),
            (WorkerId::from(2), vec![0, 1]),
            (WorkerId::from(3), vec![0, 1]),
        ]
    }

    fn drain(policy: PlacementPolicy) -> Vec<u64> {
        let mut a = TaskSlotAssigner::new(three_workers(), policy);
        // 6 tasks (== total slots) — record which worker each lands on.
        (0..6)
            .map(|_| u64::from(a.next().unwrap().0))
            .collect()
    }

    // VAJ-BF2 T-BF2.5: fill-first PACKS a stage onto one worker (the T2-measured "all on one pod");
    // even-spread FANS OUT one-per-worker (Flink evenly-spread-out-slots) so the N partitions
    // distribute. This is the difference that makes a cut streaming stage actually distribute.
    #[test]
    fn fill_first_packs_one_worker_before_the_next() {
        // Worker 1's two slots drain fully before worker 2, etc.
        assert_eq!(drain(PlacementPolicy::FillFirst), vec![1, 1, 2, 2, 3, 3]);
    }

    #[test]
    fn even_spread_fans_out_across_workers() {
        // Round-robin across equal-capacity workers: 1,2,3 then 1,2,3.
        assert_eq!(drain(PlacementPolicy::EvenSpread), vec![1, 2, 3, 1, 2, 3]);
    }

    #[test]
    fn even_spread_prefers_the_worker_with_most_free_slots() {
        // Worker 2 starts with more free slots -> it is chosen first until balanced.
        let slots = vec![
            (WorkerId::from(1), vec![0]),
            (WorkerId::from(2), vec![0, 1, 2]),
        ];
        let mut a = TaskSlotAssigner::new(slots, PlacementPolicy::EvenSpread);
        let seq: Vec<u64> = (0..4).map(|_| u64::from(a.next().unwrap().0)).collect();
        // w2(3) > w1(1): pick w2 -> now w2(2)>w1(1): w2 -> w2(1)==w1(1): first index (w1) ->
        // remaining w2(1): w2. Net: 2,2,1,2 (spreads load onto the emptier-later worker w2).
        assert_eq!(seq, vec![2, 2, 1, 2]);
    }
}
