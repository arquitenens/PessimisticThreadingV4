use std::any::{Any, TypeId};
use std::cell::UnsafeCell;
use std::intrinsics::transmute;
use std::marker::PhantomData;
use std::mem::{transmute_copy, ManuallyDrop};
use std::ptr;
use std::ptr::{drop_in_place, NonNull};
use std::sync::Arc;
use crate::builder::SubScheduler;




pub(crate) trait Task{
    unsafe fn execute(this: *const ());
    unsafe fn drop(this: *const ());
}

#[derive(Clone)]
//#[repr(align(64))]
#[repr(C)]
pub(crate) struct TypelessTask {
    pub(crate) callable: unsafe fn(*const ()),
    pub(crate) data: *const (),

    pub(crate) signature: TypeId,
}
unsafe impl Send for TypelessTask {}

pub const TYPELESS_SIZE: usize = size_of::<TypelessTask>().next_power_of_two();


//Needs to be private
struct NoValue;
impl Default for TypelessTask {

    fn default() -> Self {
        TypelessTask{
            //Garbage data, will be overwritten anyway
            //The signature shouldn't be a representable type so it doesnt get matched
            //since this is not a correct Task
            signature: TypeId::of::<NoValue>(),
            data: ptr::null(),
            callable: unsafe {|_|{}}
        }
    }
}



impl TypelessTask {
    //data must be valid til executed
    pub unsafe fn new<T, S: 'static>(data: *const T) -> TypelessTask where T: Task{
        TypelessTask {
            callable: <T as Task>::execute,
            data: data as *const _,
            signature: TypeId::of::<S>(),
        }
    }
    pub unsafe fn execute(&mut self){
        unsafe { (self.callable)(self.data) }
    }
    pub unsafe fn drop(this: unsafe fn(*const ()), data: *const ()) {
        drop_in_place(this as *mut ());
        drop_in_place(data as *mut ());
    }
}

impl<F: FnMut()> Task for F {
    unsafe fn execute(this: *const ()) {
        unsafe { (*(this as *mut F))() }
    }
    unsafe fn drop(this: *const ()) {
        unsafe { drop(Box::from_raw(this as *mut F)) }
    }
}

