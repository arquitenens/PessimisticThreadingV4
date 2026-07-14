use std::mem::MaybeUninit;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use heapless::spsc::Consumer;
use crate::task::{TaskConsumer, TypelessTask};

pub(crate) struct Worker{
    pub(crate) worker_state: Arc<AtomicUsize>,
    //the worker doesn't care if the task came from cache or from the queue
    pub(crate) current_task: Consumer<'static, TypelessTask>
}

impl Worker {
    pub(crate) fn new(state: Arc<AtomicUsize>, consumer: TaskConsumer){

    }
}
