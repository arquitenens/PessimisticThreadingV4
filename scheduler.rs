use crate::builder::{SchedulerBuilder, TypesIdx, MAX_SCHEDULERS};
use crate::scheduler_config::Config;
use crate::task::{Task, TypelessTask};
use crate::worker::{SendPtr, Worker};
use std::any::TypeId;
use std::cell::RefCell;
use std::mem::MaybeUninit;
use std::ptr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::{Arc, RwLock};
use std::sync::atomic::Ordering::{Acquire, Release};
use std::thread::JoinHandle;
use crossbeam_utils::CachePadded;

pub(crate) struct FIDLookUp{
    pub(crate) entries: [(TypeId, usize); 16],
    pub(crate) cursor: usize,
}
impl FIDLookUp {
    #[inline(always)]
    fn get(&self, k: &TypeId) -> Option<usize> {
        self.entries.iter().find(|(t, _)| *t == *k).map(|&(_, i)| i)
    }
    #[inline(always)]
    fn insert(&mut self, k: TypeId){
        self.entries[self.cursor] = (k, self.cursor);
        self.cursor += 1;
    }
}

pub(crate) const MAX_WORKERS_PER_SCHED: usize = 64;

//More errors i dont care
pub enum WorkerErrors<F: FnMut() + Send + 'static>{
    Busy(F),
    Misc
}

pub struct Scheduler{
    //type id here is for the Task/Function not for the scheduler
    pub(crate) reusable_tasks: FIDLookUp,
    pub(crate) generic_schedulers: Vec<Box<SubScheduler>>,
    //usize is the element count
    pub(crate) task_wrapper: ([*mut TypelessTask; MAX_WORKERS_PER_SCHED], usize),

}


impl Scheduler {
    pub fn new(config: Config) -> SchedulerBuilder {

        return SchedulerBuilder{
            generic_schedulers: Vec::default(),
            config,
            total: 0,
            sub_scheduler_amount: 0,
            inner: MaybeUninit::uninit()
        };
    }

    ///Get any
    pub fn any<T, F>
    (&mut self, exec: F) -> Result<(), WorkerErrors<F>>
    where F: FnMut() + Send + 'static,
          T: 'static + TypesIdx,
    {

        if T::tid_idx() >= MAX_SCHEDULERS{
            std::hint::cold_path();
            panic!("too large");
        }

        let inner: &mut Box<SubScheduler> = &mut self.generic_schedulers[T::tid_idx()];

        let available_workers = inner.worker_state.load(Ordering::Acquire);
        //println!("available workers: {:b}", available_workers);
        if available_workers == 0 {
            return Err(WorkerErrors::Busy(exec));
        }

        let available_idx = available_workers.trailing_zeros() as usize;
        //println!("workers: {:064b}", available_workers);
        //println!("available: {:?}, scheduler: {}", available_idx, T::tid_idx());

        //TODO prefetch count using condvar

        inner.worker_state.fetch_and(!(1u64 << available_idx), Ordering::Release);

        let tid_of_task = TypeId::of::<F>();


        //TODO fix caching
        if let Some(idx) = self.reusable_tasks.get(&tid_of_task){
            let cached_task = self.task_wrapper.0[idx].clone();
            let local = available_idx - inner.offset;
            let _ = inner.producer[local].swap(cached_task, Ordering::Release);
            //std::sync::atomic::compiler_fence(Ordering::Release);
            return Ok(());
        };

        //dbg!("Non-Cached task pushed");

        let raw_fn: *const F = Box::into_raw(Box::new(exec));
        let task = Box::into_raw(Box::new(unsafe {TypelessTask::new::<F, F>(raw_fn)}));
        let amount = self.task_wrapper.1;
        self.task_wrapper.0[amount % self.task_wrapper.0.len()] = task.clone();
        self.reusable_tasks.insert(tid_of_task);
        self.task_wrapper.1 += 1;

        let local = available_idx - inner.offset;
        unsafe {inner.producer[local].swap(task, Ordering::Release);};
        //fence prob not needed?
        //std::sync::atomic::compiler_fence(Ordering::Release);
        Ok(())
    }

}
#[repr(C)]
pub struct SubScheduler{
    pub(crate) producer: [CachePadded<AtomicPtr<TypelessTask>>; MAX_WORKERS_PER_SCHED],
    pub(crate) thread_stop: [AtomicBool; MAX_WORKERS_PER_SCHED],
    pub(crate) handles: Vec<JoinHandle<()>>,
    pub(crate) idx: usize,
    pub(crate) offset: usize,
    pub(crate) workers: usize,
    //TODO this might not even need to be atomic at all!
    //TODO it is shared and will cause a race condition but its not actually an issue
    pub(crate) worker_state: CachePadded<Arc<AtomicU64>>,
}

impl Drop for SubScheduler {
    //this might drop a non-finished task, its on you to make sure the scheduler lives long enough
    //for every task to complete if you care
    fn drop(&mut self) {
        for (idx, h) in self.handles.drain(..).enumerate(){
            self.thread_stop[idx].store(true, Ordering::Release);
            h.join().unwrap();
        }
    }
}

thread_local!(static COUNTER: RefCell<usize> = RefCell::new(0));

impl SubScheduler {
    pub fn new(workers: usize, idx: usize, offset: usize, amount: usize) -> Box<SubScheduler> {
        assert!(offset + workers <= 64, "capacity used up, try less threads, less workers or make a new scheduler!");
        let mut zero = 0u64;
        zero |= (1 << (workers + offset)) - 1;
        zero &= !((1 << (offset)) - 1);

        let worker_state = Arc::new(AtomicU64::new(zero));
        //println!("worker_state: {:064b}", worker_state.load(Ordering::Acquire));
        
        let producers =
            [const {CachePadded::new(AtomicPtr::new(null_mut::<TypelessTask>()))}; MAX_WORKERS_PER_SCHED];
        let handles = Vec::with_capacity(MAX_WORKERS_PER_SCHED);
        let thread_stop = [const {AtomicBool::new(false)}; MAX_WORKERS_PER_SCHED];

        let mut incomplete_sub = Box::new(SubScheduler{
            idx,
            //usize::MAX == all workers available
            offset,
            workers,
            worker_state: CachePadded::new(worker_state.clone()),
            producer: producers,
            thread_stop,
            handles,
        });



        let cores = core_affinity::get_core_ids().unwrap();
        for worker in 0..amount{

            let sig = SendPtr(ptr::from_ref(&incomplete_sub.thread_stop[worker]) as *mut _);

            let swapped = SendPtr(ptr::from_mut(&mut incomplete_sub.producer[worker]));

            let w = Worker::new(worker_state.clone(), swapped, worker, sig, offset);


            let amount = COUNTER.with(|x| x.borrow().clone());
            let clone = cores.clone();
            let handle = std::thread::spawn(move || {
                core_affinity::set_for_current(clone[amount]);
                w.execute()
            });
            COUNTER.with_borrow_mut(|x| *x = (*x + 1) % cores.len());
            incomplete_sub.handles.push(handle);
        }


        incomplete_sub.worker_state = CachePadded::new(worker_state);

        return incomplete_sub
    }
}
