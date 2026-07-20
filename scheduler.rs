use crate::builder::{SchedulerBuilder, TypesIdx, MAX_SCHEDULERS};
use crate::scheduler_config::Config;
use crate::task::{Task, TypelessTask};
use crate::worker::{SendPtr, Worker};
use std::any::TypeId;
use std::collections::HashMap;
use std::mem::MaybeUninit;
use std::ptr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread::JoinHandle;
use std::time::Instant;

pub(crate) const MAX_WORKERS_PER_SCHED: usize = 64;

pub struct Scheduler{
    pub generic_schedulers: Vec<Box<SubScheduler>>,
    //type id here is for the Task/Function not for the scheduler
    pub reusable_tasks: Arc<HashMap<TypeId, usize>>,
    //usize is the element count
    //TODO maybe wrap in RwLock for mutability on some arguments? removed for now, dont know how to implement
    pub task_wrapper: ([*mut TypelessTask; MAX_WORKERS_PER_SCHED], usize),

    pub config: Config
}


impl Scheduler {
    pub fn new(config: Config) -> SchedulerBuilder {

        return SchedulerBuilder{
            parent: ptr::null_mut(),
            generic_schedulers: Vec::default(),
            config,
            queue: MaybeUninit::uninit(),
            sub_scheduler_amount: 0,
            inner: MaybeUninit::uninit()
        };
    }

    ///Get any
    pub fn any<'self_lt, 'other_lt, T, F>
    (&'self_lt mut self, exec: F)
    where F: FnMut() + Send + 'static,
          T: 'static + TypesIdx,
          'self_lt: 'other_lt,
    {

        if T::tid_idx() >= MAX_SCHEDULERS{
            return;
        }
        let inner: &mut Box<SubScheduler> = &mut self.generic_schedulers[T::tid_idx()];

        //TODO non atomic way is SIMD masking 512bit reg using roughly vpmovmskb to turn a [bool; N] into a bitmask
        let available_workers = inner.worker_state.load(Ordering::Acquire);
        //println!("available workers: {:b}", available_workers);
        if available_workers == 0 {
            return;
        }

        let available_idx = available_workers.trailing_zeros() as usize;

        let mut temp = available_workers;
        //TODO prefetch count
        #[cfg(feature = "?")]
        for _ in 0..4{
            if temp == 0 {
                break;
            }
            let pre_idx = temp.trailing_zeros() as usize;
            temp &= !(1u64 << pre_idx);
            //availabe.workers[pre_idx].notify_one
        }
        inner.worker_state.fetch_and(!(1u64 << available_idx), Ordering::Release);

        let tid_of_task = TypeId::of::<F>();

        if let Some(idx) = self.reusable_tasks.get(&tid_of_task){
            let cached_task = &mut unsafe {(*self.task_wrapper.0[*idx]).clone()};
            unsafe {inner.producer[available_idx].swap(cached_task, Ordering::Release);};
            std::sync::atomic::compiler_fence(Ordering::Release);
            return;
        };

        let raw_fn: *const F = Box::into_raw(Box::new(exec));
        let task = &mut unsafe {TypelessTask::new::<F, F>(raw_fn)};
        let amount = self.task_wrapper.1;
        self.task_wrapper.0[amount % self.task_wrapper.0.len()] = &mut task.clone();
        self.task_wrapper.1 += 1;

        //TODO this is very weird... technically ub? but its logically sound and consistent
        //TODO honestly I don't know how the cpu treats this, if its causing weird stuff just replace with atomic ptr + write
        unsafe {inner.producer[available_idx].swap(task, Ordering::Release);};
        //fence prob not needed?
        std::sync::atomic::compiler_fence(Ordering::Release);
    }

}

pub struct SubScheduler{
    pub(crate) parent: AtomicPtr<Scheduler>,
    pub(crate) idx: usize,
    pub(crate) producer: [AtomicPtr<TypelessTask>; MAX_WORKERS_PER_SCHED],
    pub(crate) thread_stop: [AtomicBool; MAX_WORKERS_PER_SCHED],
    pub(crate) handles: Vec<JoinHandle<()>>,

    //TODO this might not even need to be atomic at all!
    //TODO it is shared and will cause a race condition but its not actually an issue
    pub(crate) worker_state: Arc<AtomicU64>,
}

impl Drop for SubScheduler {
    fn drop(&mut self) {
        for (idx, h) in self.handles.drain(..).enumerate(){
            self.thread_stop[idx].store(true, Ordering::Release);
            h.join().unwrap();
        }
    }
}

impl SubScheduler {
    pub fn new(workers: usize, idx: usize) -> Box<SubScheduler> {


        let worker_state = Arc::new(AtomicU64::new((1 << workers as u64) - 1));
        //println!("worker_state: {}", worker_state.load(Ordering::Acquire));
        
        let producers = [const {AtomicPtr::new(null_mut::<TypelessTask>())}; MAX_WORKERS_PER_SCHED];
        let handles = Vec::with_capacity(MAX_WORKERS_PER_SCHED);
        let thread_stop = [const {AtomicBool::new(false)}; MAX_WORKERS_PER_SCHED];

        let mut incomplete_sub = Box::new(SubScheduler{
            parent: AtomicPtr::new(null_mut()),
            idx,
            //usize::MAX == all workers available
            worker_state: worker_state.clone(),
            producer: producers,
            thread_stop,
            handles,
        });

        for worker in 0..workers{

            //Tx is not valid yet, its important that its None
            let sig = SendPtr(ptr::from_ref(&incomplete_sub.thread_stop[worker]) as *mut _);

            let null: *mut TypelessTask = null_mut();

            let w = Worker::new(worker_state.clone(), AtomicPtr::from(incomplete_sub.producer[worker].load(Acquire)), worker, sig);
            let handle = std::thread::spawn(move || {
                w.execute()
            });
            incomplete_sub.handles.push(handle);
        }


        incomplete_sub.worker_state = worker_state;

        return incomplete_sub
    }
}
