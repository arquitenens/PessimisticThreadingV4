use std::ptr::null_mut;
use crate::task::TypelessTask;
use std::sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering};
use std::sync::Arc;

#[cfg(not(target_has_atomic = "ptr"))]
pub(crate) struct SyncCell{
    inner: UnsafeCell<[bool; MAX_WORKERS_PER_SCHED]>
}
#[cfg(not(target_has_atomic = "ptr"))]
unsafe impl Send for SyncCell {}
#[cfg(not(target_has_atomic = "ptr"))]
unsafe impl Sync for SyncCell {}

#[repr(transparent)]
pub struct SendPtr<T>(pub *mut T);
unsafe impl<T> Send for SendPtr<T> {}


pub(crate) struct Worker{
    //If no atomic support -> worker_state: Arc<SyncCell>
    #[cfg(target_has_atomic = "ptr")]
    pub(crate) worker_state: Arc<AtomicU64>,

    pub(crate) idx: usize,

    pub(crate) signal: SendPtr<AtomicBool>,

    #[cfg(not(target_has_atomic = "ptr"))]
    pub(crate) worker_state: Arc<SyncCell>,
    //the worker doesn't care if the task came from cache or from the queue
    pub(crate) current_task: AtomicPtr<TypelessTask>
}

impl Worker {
    pub(crate) fn new(state: Arc<AtomicU64>, consumer: AtomicPtr<TypelessTask>, idx: usize, signal: SendPtr<AtomicBool>) -> Self{
        return Worker{
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
            std::sync::atomic::compiler_fence(Ordering::SeqCst);
            unsafe {
                if (*self.signal.0).load(Ordering::Acquire) {
                    if !self.current_task.load(Ordering::Acquire).is_null() {
                        let task: *mut TypelessTask = null_mut();
                        task.swap(self.current_task.load(Ordering::Acquire));
                        TypelessTask::drop((*task).callable, (*task).data)
                    }
                    break;
                }
                if self.current_task.load(Ordering::Acquire).is_null() {
                    std::hint::spin_loop();
                    continue;
                }
                let task = self.current_task.swap(null_mut(), Ordering::Acquire);
                if task.is_null() {
                    std::hint::spin_loop();
                    continue;
                }
                unsafe { (*task).execute(); }
            }
            self.worker_state.fetch_or(1 << self.idx, Ordering::Release);
        }
    }
}
