
use crate::scheduler_config::AnyHandler;
use crate::{impl_any_handler, AnyArg};
use crate::Any;
use std::any::TypeId;
use std::cell::UnsafeCell;
use std::collections::HashMap;
use std::io::BufRead;
use std::mem::{transmute, MaybeUninit};
use std::pin::Pin;
use std::ptr;
use std::sync::{Arc, RwLock};
use std::sync::atomic::AtomicPtr;
use heapless::spsc::{Consumer, Producer, Queue};
use crate::scheduler::MAX_WORKERS_PER_SCHED;
pub(crate) use crate::scheduler::Scheduler;
pub(crate) use crate::scheduler::SubScheduler;
use crate::scheduler_config::Config;
use crate::task::{TaskConsumer, TaskProducer, TypelessTask, TYPELESS_SIZE};


//TODO align struct fields
pub struct SchedulerBuilder{
    pub(crate) parent: *mut Scheduler,
    pub(crate) generic_schedulers: HashMap<TypeId, Vec<SubScheduler>>,
    pub(crate) config: Config,
    pub(crate) sub_scheduler_amount: usize,
    pub(crate) queue: MaybeUninit<[Option<(TaskProducer, TaskConsumer)>; MAX_WORKERS_PER_SCHED]>,
    pub(crate) inner: MaybeUninit<Scheduler>
}

impl_any_handler!();
impl_any_handler!(A);
impl_any_handler!(A, B);
impl_any_handler!(A, B, C);
impl_any_handler!(A, B, C, D);
impl_any_handler!(A, B, C, D, E);
impl_any_handler!(A, B, C, D, E, F);

//if you need more than 7 arguments in your function youre mad and should split up your function
impl_any_handler!(A, B, C, D, E, F, G);

impl SchedulerBuilder {
    pub fn add_scheduler<T: Any>(mut self) -> Self {
        let type_id = TypeId::of::<T>();


        println!("type_id T: {:?}", &TypeId::of::<T>());

        let mut queue_buffer: MaybeUninit<[Option<(TaskProducer, TaskConsumer)>; MAX_WORKERS_PER_SCHED]> = MaybeUninit::uninit();

        for i in 0..MAX_WORKERS_PER_SCHED {
            let queue = Arc::new(UnsafeCell::new(heapless::spsc::Queue::<_, TYPELESS_SIZE>::new()));
            let ptr = queue.get();
            let (tx, rx) = unsafe {&mut (*ptr)}.split();
            //the lifetime is kept alive by the Arc queue so changing the lifetime is okay

            let tx: Producer<'static, TypelessTask> = unsafe {transmute(tx)};
            let rx: Consumer<'static, TypelessTask> = unsafe {transmute(rx)};

            let prod = TaskProducer{
                producer: tx,
                _queue: queue.clone()
            };
            let cons = TaskConsumer{
                consumer: rx,
                _queue: queue.clone()
            };
            unsafe {queue_buffer.assume_init_mut()[i] = Some((prod, cons))};
        }


        self.generic_schedulers.insert(type_id, vec![
            SubScheduler::new(&mut queue_buffer, self.config.threads_per_worker, self.sub_scheduler_amount)
        ]);
        self.sub_scheduler_amount += 1;
        return self
    }

    pub fn apply(mut self) -> Scheduler{

        let mut task_wrapper: [MaybeUninit<_>; MAX_WORKERS_PER_SCHED] =
            [const {MaybeUninit::uninit()}; MAX_WORKERS_PER_SCHED];
        task_wrapper.iter_mut().for_each(|t| {
            t.write(Arc::new(RwLock::new(TypelessTask::default())));
        });

        self.inner = MaybeUninit::new(Scheduler{
            config: self.config,
            generic_schedulers: self.generic_schedulers,
            reusable_tasks: Arc::new(HashMap::new()),
            task_wrapper: (unsafe {transmute(task_wrapper)}, 0),
            queue_holder: unsafe {self.queue.assume_init()},
        });
        return unsafe {self.inner.assume_init()}
    }
}
