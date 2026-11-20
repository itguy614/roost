use std::collections::{HashMap, HashSet};

use roost_core::{
    ProjectId,
    scheduler::SchedulerIntent,
    worker_runtime::{LaunchIntent, ProjectIntent},
};
use roost_ipc::{
    Mutation, RpcError,
    groups::{
        GroupLaunchParams, GroupLifecycle, GroupOutcome, GroupProjectLaunch, GroupProjectResult,
        GroupStopParams,
    },
};
use roost_store::Store;
use serde_json::Value;

use super::{at_revision, group};
use crate::{
    encode, request_key, store_error,
    supervisor::{Error, Plan, SchedulerPlan, Supervisor},
    worker_rpc::{worker_error, worker_mutation},
};

struct Start {
    workers: Option<Plan>,
    scheduler: Option<SchedulerPlan>,
    allow_horizon: bool,
}

impl Start {
    fn prepare(
        supervisor: &Supervisor,
        store: &Store,
        id: ProjectId,
        launch: Option<&GroupProjectLaunch>,
        restart: bool,
    ) -> Result<Self, Error> {
        let workers = store
            .list_worker_profiles(id)?
            .iter()
            .any(|profile| profile.enabled && profile.desired_instances > 0);
        let scheduler = store
            .project_scheduler(id)?
            .is_some_and(|profile| profile.enabled);
        if !workers && !scheduler {
            return Ok(Self {
                workers: None,
                scheduler: None,
                allow_horizon: false,
            });
        }
        let launch = launch.ok_or(Error::Invalid(
            "no resolved PHP executable supplied for this project",
        ))?;
        Ok(Self {
            workers: if workers {
                supervisor.prepare(store, id, &launch.php, launch.allow_horizon, restart)?
            } else {
                None
            },
            scheduler: if scheduler {
                supervisor.prepare_scheduler(store, id, &launch.php, restart)?
            } else {
                None
            },
            allow_horizon: launch.allow_horizon,
        })
    }

    fn persist(&self, store: &Store, id: ProjectId) -> Result<GroupOutcome, Error> {
        if let Some(plan) = &self.workers {
            store.set_project_intent(
                id,
                &ProjectIntent::Running(LaunchIntent {
                    php: plan.php().to_owned(),
                    allow_horizon: self.allow_horizon,
                }),
            )?;
        }
        if let Some(plan) = &self.scheduler {
            store.set_scheduler_intent(
                id,
                &SchedulerIntent::Running {
                    php: plan.php.clone(),
                },
            )?;
        }
        Ok(if self.workers.is_none() && self.scheduler.is_none() {
            GroupOutcome::Unchanged
        } else {
            GroupOutcome::Accepted {
                workers: self.workers.is_some(),
                scheduler: self.scheduler.is_some(),
            }
        })
    }

    fn apply(self, supervisor: &mut Supervisor, id: ProjectId) {
        if let Some(plan) = self.workers {
            supervisor.apply_start(id, plan);
        }
        if let Some(plan) = self.scheduler {
            supervisor.start_scheduler(id, plan);
        }
    }
}

pub(super) fn launch(
    supervisor: &mut Supervisor,
    store: &Store,
    params: GroupLaunchParams,
    restart: bool,
) -> Result<Value, RpcError> {
    let key = request_key(
        if restart {
            "group.restart"
        } else {
            "group.start"
        },
        &params,
    )?;
    let members = store.group_projects(params.id).map_err(store_error)?;
    for member in &members {
        supervisor
            .clean_previous(store, member.id)
            .map_err(worker_error)?;
        supervisor
            .clean_previous_scheduler(store, member.id)
            .map_err(worker_error)?;
    }
    let mut plans = Vec::new();
    let (operation_id, result) = worker_mutation(store, params.idempotency_key, &key, || {
        at_revision(store, params.id, params.expected_revision)?;
        let ids: HashSet<_> = members.iter().map(|project| project.id).collect();
        let mut launches = HashMap::new();
        for launch in &params.launches {
            if !ids.contains(&launch.id) || launches.insert(launch.id, launch).is_some() {
                return Err(Error::Invalid(
                    "launches must contain distinct group member IDs",
                ));
            }
        }
        let mut capacity = supervisor.available_capacity();
        let mut projects = Vec::new();
        for member in members {
            let id = member.id;
            let prepared =
                Start::prepare(supervisor, store, id, launches.get(&id).copied(), restart)
                    .and_then(|plan| {
                        let growth = supervisor.additional_capacity(
                            id,
                            plan.workers.as_ref(),
                            plan.scheduler.as_ref(),
                        );
                        capacity = capacity.checked_sub(growth).ok_or(Error::Capacity)?;
                        Ok(plan)
                    });
            let outcome = match prepared {
                Ok(plan) => {
                    let outcome = plan.persist(store, id)?;
                    plans.push((id, plan));
                    outcome
                }
                Err(error @ Error::Store(_)) => return Err(error),
                Err(error) => GroupOutcome::Failed {
                    error: worker_error(error),
                },
            };
            projects.push(GroupProjectResult { id, outcome });
        }
        Ok(GroupLifecycle {
            id: params.id,
            projects,
        })
    })?;
    for (id, plan) in plans {
        plan.apply(supervisor, id);
    }
    encode(Mutation {
        operation_id,
        result,
    })
}

pub(super) fn stop(
    supervisor: &mut Supervisor,
    store: &Store,
    params: GroupStopParams,
) -> Result<Value, RpcError> {
    let key = request_key("group.stop", &params)?;
    let mut stopped = Vec::new();
    let (operation_id, result) = worker_mutation(store, params.idempotency_key, &key, || {
        group(store, params.id)?;
        let mut projects = Vec::new();
        for member in store.group_projects(params.id)? {
            let id = member.id;
            let scheduler = store.project_scheduler(id)?.is_some();
            store.set_project_intent(id, &ProjectIntent::Stopped)?;
            if scheduler {
                store.set_scheduler_intent(id, &SchedulerIntent::Stopped)?;
            }
            stopped.push((id, scheduler));
            projects.push(GroupProjectResult {
                id,
                outcome: GroupOutcome::Accepted {
                    workers: true,
                    scheduler,
                },
            });
        }
        Ok(GroupLifecycle {
            id: params.id,
            projects,
        })
    })?;
    for (id, scheduler) in stopped {
        supervisor.apply_stop(id);
        if scheduler {
            supervisor.stop_scheduler(id);
        }
    }
    encode(Mutation {
        operation_id,
        result,
    })
}
