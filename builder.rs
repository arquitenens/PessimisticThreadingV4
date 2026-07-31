
use std::any::{Any, TypeId};
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
use crate::scheduler::{FIDLookUp, MAX_WORKERS_PER_SCHED};
pub(crate) use crate::scheduler::Scheduler;
pub(crate) use crate::scheduler::SubScheduler;
use crate::scheduler_config::Config;
use crate::task::TypelessTask;
use crate::worker::SendPtr;

//TODO align struct fields
pub struct SchedulerBuilder{
    pub(crate) generic_schedulers: Vec<Box<SubScheduler>>,
    pub(crate) config: Config,
    pub(crate) total: usize,
    pub(crate) sub_scheduler_amount: usize,
    pub(crate) inner: MaybeUninit<Scheduler>
}

pub const MAX_SCHEDULERS: usize = 16;
pub trait TypesIdx{
    fn tid_idx() -> usize;
}



pub enum ThreadAmount{
    Overwrite(usize),
    Default
}
impl SchedulerBuilder {
    pub fn add_scheduler<T: Any>(mut self, thread_overwrite: ThreadAmount) -> Self {
        let mut amount = self.config.threads_per_sub_sched;
        let threads_for_this = match thread_overwrite {
            ThreadAmount::Overwrite(x) => {
                amount = x;
                self.config.threads_per_sub_sched + x
            },
            ThreadAmount::Default => self.config.threads_per_sub_sched,
        };

        let offset = self.total;
        self.total += threads_for_this;


        self.generic_schedulers.push(SubScheduler::new(self.config.threads_per_sub_sched, self.sub_scheduler_amount, offset, amount));

        self.sub_scheduler_amount += 1;
        return self
    }

    pub fn apply(mut self) -> Scheduler{
        
        self.inner = MaybeUninit::new(Scheduler{
            generic_schedulers: self.generic_schedulers,
            reusable_tasks: FIDLookUp {
                entries: const {[(TypeId::of::<i32>(), 0); 16]},
                cursor: 0,
            },
            task_wrapper: ([const {null_mut::<TypelessTask>()}; 64], 0),
        });
        return unsafe {self.inner.assume_init()}
    }
}
