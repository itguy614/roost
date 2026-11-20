use std::{io, path::Path, process::Command};

use roost_core::{
    CrashRestartPolicy, Project, WorkerProfile, WorkerProfileId,
    scheduler::{SchedulerProfile, SchedulerProfileId},
    worker_runtime::ProcessProfileId,
};
use roost_store::{Error, Store};

pub(crate) trait SupervisedProfile {
    type Id: ProcessProfileId;

    fn id(&self) -> Self::Id;
    fn load(store: &Store, id: Self::Id) -> Result<Option<Self>, Error>
    where
        Self: Sized;
    fn crash_restart(&self) -> CrashRestartPolicy;
    fn grace_seconds(&self) -> u32;
    fn command(&self, project: &Project, php: &Path) -> io::Result<Command>;
}

impl SupervisedProfile for WorkerProfile {
    type Id = WorkerProfileId;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn load(store: &Store, id: Self::Id) -> Result<Option<Self>, Error> {
        store.get_worker_profile(id)
    }

    fn crash_restart(&self) -> CrashRestartPolicy {
        self.crash_restart
    }

    fn grace_seconds(&self) -> u32 {
        self.graceful_stop_seconds
    }

    fn command(&self, project: &Project, php: &Path) -> io::Result<Command> {
        let php = roost_process::validate_worker(project, self, php)?;
        Ok(roost_process::worker_command(project, self, &php))
    }
}

impl SupervisedProfile for SchedulerProfile {
    type Id = SchedulerProfileId;

    fn id(&self) -> Self::Id {
        self.id
    }

    fn load(store: &Store, id: Self::Id) -> Result<Option<Self>, Error> {
        store.scheduler(id)
    }

    fn crash_restart(&self) -> CrashRestartPolicy {
        self.crash_restart
    }

    fn grace_seconds(&self) -> u32 {
        self.graceful_stop_seconds
    }

    fn command(&self, project: &Project, php: &Path) -> io::Result<Command> {
        let php = roost_process::validate_scheduler(project, self, php)?;
        Ok(roost_process::scheduler_command(project, &php))
    }
}
