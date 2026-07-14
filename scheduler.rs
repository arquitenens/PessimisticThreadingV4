use crate::builder::SchedulerBuilder;
use crate::scheduler_config::{AnyHandler, Config};
use crate::task::{AnyArg, TaskConsumer, TaskProducer, TypelessTask};
use crate::worker::Worker;
use std::any::{Any, TypeId};
use std::collections::HashMap;
use std::env::Args;
use std::marker::PhantomData;
use std::mem::{ManuallyDrop, MaybeUninit};
use std::ops::Range;
use std::ptr;
use std::ptr::null_mut;
use std::sync::atomic::{AtomicPtr, AtomicU64, AtomicUsize};
use std::sync::{Arc, RwLock};



pub(crate) const MAX_WORKERS_PER_SCHED: usize = 32;

pub struct Scheduler{
    pub generic_schedulers: HashMap<TypeId, Vec<SubScheduler>>,
    //type id here is for the Task/Function not for the scheduler
    pub reusable_tasks: Arc<HashMap<TypeId, usize>>,
    //usize is the element count
    pub task_wrapper: ([Arc<RwLock<TypelessTask>>; MAX_WORKERS_PER_SCHED], usize),

    pub queue_holder: [Option<(TaskProducer, TaskConsumer)>; MAX_WORKERS_PER_SCHED],

    pub config: Config
}


pub struct SelfHandle<'a, T: Send>{
    pub val: *const T,
    //TODO dont know about ManuallyDrop
    pub _phantom: PhantomData<&'a ManuallyDrop<T>>
}
impl<'a, T: Send> SelfHandle<'a, T> {

    ///ref makes sure you have no exclusive references anymore
    ///make sure it lives as long as T
    //TODO prob redesign
    pub unsafe fn new<'guard>(self_: &'a T) -> Self where 'a: 'guard {
        Self{
            val: ptr::from_ref(self_),
            _phantom: PhantomData
        }
    }

    fn call(&mut self){
        //TODO probably not gonna use this
    }
}



impl Scheduler {
    pub fn new(config: Config) -> SchedulerBuilder {

        return SchedulerBuilder{
            parent: ptr::null_mut(),
            generic_schedulers: HashMap::default(),
            config,
            queue: MaybeUninit::uninit(),
            sub_scheduler_amount: 0,
            inner: MaybeUninit::uninit()
        };
    }

    ///Get any
    pub async fn any<'self_lt, 'other_lt, S, F, R, Args>(&'self_lt mut self, this: SelfHandle<'other_lt, S>, args: Vec<Box<dyn Any>>, exec: F)
    where F: AnyHandler<Args, Output = R> + Send + 'static,
          S: Send + 'static,
          Args: AnyArg + 'static + Send,
          'self_lt: 'other_lt,
    {


        let matches = exec.matching_arguments(args.as_slice());
        dbg!(matches);

        let arg_len = args.len();


        //let tid_of_task = &TypeId::of::<F>();
        let inner = match self.generic_schedulers.get_mut(&TypeId::of::<Args>()){
            Some(inner) => inner,
            None => {
                //dbg!(&TypeId::of::<Args>());
                &mut Vec::new()
            }
        };


        let selected = Scheduler::get_available_single(inner);

        //TODO use task id to manage caching
        //at the time of calling the producers should have already been successfully initialized, thus you can assume so
        //TODO somehow turn F into TypelessTask
    }

    fn args_match<T: AnyArg>(any_args: Vec<Box<dyn Any>>, args: T) -> bool{



        for arg in any_args.iter(){

        }
        todo!();
    }

    fn get_available_single<'a>(of: &mut Vec<SubScheduler>) ->  Option<*mut SubScheduler> {

        return None
    }
}

pub struct SubScheduler{
    pub(crate) parent: AtomicPtr<Scheduler>,
    pub(crate) idx: usize,
    pub(crate) reusable_task: Arc<RwLock<TypelessTask>>,
    //not actually static, im just a liar
    //though it will live as long as the Scheduler itself
    pub(crate) producer: Arc<[TaskProducer]>,


    //TODO this might not even need to be atomic at all!
    //TODO it is shared and will cause a race condition but its not actually an issue?
    pub(crate) worker_state: Arc<AtomicU64>,
}

impl SubScheduler {
    pub fn new(queue: &mut MaybeUninit<[Option<(TaskProducer, TaskConsumer)>; MAX_WORKERS_PER_SCHED]>, workers: usize, idx: usize) -> SubScheduler {


        let worker_state = Arc::new(AtomicUsize::new(0));
        
        let mut producers = Vec::new();
        
        for worker in 0..workers{
            let (tx, rx) = unsafe {queue.assume_init_mut()[worker].take().unwrap()};
            producers.push(tx);
            Worker::new(worker_state.clone(), rx);
        }

        return SubScheduler{
            parent: AtomicPtr::new(null_mut()),
            idx,
            //usize::MAX == all workers available
            worker_state: Arc::new(AtomicU64::new(u64::MAX)),
            reusable_task: Arc::new(
                RwLock::new(
                    TypelessTask::default()
                )),
            producer: producers.into(),
        };
    }
}
