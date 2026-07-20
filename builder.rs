
use crate::Any;
use std::any::TypeId;
use std::cell::{OnceCell, UnsafeCell};
use std::collections::HashMap;
use std::hash::Hash;
use std::hint::black_box;
use std::io::BufRead;
use std::mem::{transmute, MaybeUninit};
use std::pin::Pin;
use std::ptr;
use std::ptr::{addr_of_mut, null_mut};
use std::sync::{Arc, OnceLock, RwLock};
use std::sync::atomic::{AtomicPtr, AtomicUsize, Ordering};
use crate::scheduler::MAX_WORKERS_PER_SCHED;
pub(crate) use crate::scheduler::Scheduler;
pub(crate) use crate::scheduler::SubScheduler;
use crate::scheduler_config::Config;
use crate::task::TypelessTask;
use crate::worker::SendPtr;

//TODO align struct fields
pub struct SchedulerBuilder{
    pub(crate) parent: *mut Scheduler,
    pub(crate) generic_schedulers: Vec<Box<SubScheduler>>,
    pub(crate) config: Config,
    pub(crate) sub_scheduler_amount: usize,
    pub(crate) queue: MaybeUninit<[Option<(*mut Option<TypelessTask>, *mut Option<TypelessTask>)>; MAX_WORKERS_PER_SCHED]>,
    pub(crate) inner: MaybeUninit<Scheduler>
}

pub const MAX_SCHEDULERS: usize = 16;
static NEXT_ID: AtomicUsize = AtomicUsize::new(0);
#[derive(Clone, Copy)]
pub struct Idx(pub(crate) fn() -> usize);
pub static ADD_TYPE: Idx = Idx(||{
    static ID: OnceLock<usize> = OnceLock::new();
    *ID.get_or_init(|| NEXT_ID.fetch_add(1, Ordering::Relaxed))
});
pub trait TypesIdx{
    fn tid_idx() -> usize;
}




impl SchedulerBuilder {
    pub fn add_scheduler<T: Any>(mut self) -> Self {
        //TODO increments even for the same type, i dont know if that's good or if i should do a check

        self.generic_schedulers.push(SubScheduler::new(self.config.threads_per_sub_sched, self.sub_scheduler_amount));

        self.sub_scheduler_amount += 1;
        return self
    }

    pub fn apply(mut self) -> Scheduler{
        
        self.inner = MaybeUninit::new(Scheduler{
            config: self.config,
            generic_schedulers: self.generic_schedulers,
            reusable_tasks: Arc::new(HashMap::new()),
            task_wrapper: ([const {null_mut::<TypelessTask>()}; 64], 0),
        });
        return unsafe {self.inner.assume_init()}
    }
}
