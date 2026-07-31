use std::ptr::null_mut;
use crate::task::TypelessTask;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;
use crossbeam_utils::CachePadded;

#[repr(transparent)]
pub struct SendPtr<T>(pub *mut T);
unsafe impl<T> Send for SendPtr<T> {}


pub(crate) struct Worker{
    pub(crate) worker_state: Arc<AtomicU64>,

    pub(crate) idx: usize,

    pub(crate) offset: usize,

    pub(crate) signal: SendPtr<AtomicBool>,

    //the worker doesn't care if the task came from cache or from the queue
    pub(crate) current_task: SendPtr<CachePadded<AtomicPtr<TypelessTask>>>
}

impl Worker {
    pub(crate) fn new(
        state: Arc<AtomicU64>,
        consumer: SendPtr<CachePadded<AtomicPtr<TypelessTask>>>,
        idx: usize, signal: SendPtr<AtomicBool>, offset: usize) -> Self{
        return Worker{
            offset,
            worker_state: state,
            idx,
            signal,
            current_task: consumer,
        }
    }

    pub(crate) fn execute(self){
        loop {
            //TODO #[cfg()] something something maybe maybe
            //Also add cfg flag for thread yield
            unsafe {
                if (*self.signal.0).load(Ordering::Acquire) {
                    if !(*self.current_task.0).load(Ordering::Acquire).is_null() {
                        let task = (*self.current_task.0).swap(null_mut(), Ordering::Acquire);
                        TypelessTask::drop((*task).callable, (*task).data)
                    }
                    break;
                }
                if (*self.current_task.0).load(Ordering::Acquire).is_null() {
                    //std::hint::spin_loop();
                    continue;
                }
                let task = (*self.current_task.0).swap(null_mut(), Ordering::Acquire);
                if task.is_null() {
                    //std::hint::spin_loop();
                    continue;
                }
                unsafe { (*task).execute(); }
            }

            //println!("in worker: IDX: {} OFFSET {}", self.idx, self.offset);
            self.worker_state.fetch_or(1 << (self.idx + self.offset), Ordering::Release);
        }
    }
}
