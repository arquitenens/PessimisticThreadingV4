use std::any::{Any, TypeId};
use std::cell::UnsafeCell;
use std::intrinsics::transmute;
use std::ptr;
use std::ptr::NonNull;
use std::sync::Arc;
use heapless::spsc::{Consumer, Queue};
use heapless::spsc::Producer;
use crate::builder::SubScheduler;

pub(crate) trait AnyArg: Sized{
    fn get(args: Vec<Box<dyn Any>>) -> Option<Self>;
}


impl<T: Any> AnyArg for T {
    fn get(args: Vec<Box<dyn Any>>) -> Option<Self> {
        Some(*args.into_iter().next()?.downcast::<T>().ok()?)
    }
}



pub(crate) struct TaskProducer{
    pub(crate) producer: Producer<'static, TypelessTask>,
    pub(crate) _queue: Arc<UnsafeCell<Queue<TypelessTask, TYPELESS_SIZE>>>
}

pub(crate) struct TaskConsumer{
    pub(crate) consumer: Consumer<'static, TypelessTask>,
    pub(crate) _queue: Arc<UnsafeCell<Queue<TypelessTask, TYPELESS_SIZE>>>
}

#[repr(align(16))]
#[repr(C)]
pub(crate) struct Slot{
    raw: NonNull<()>,
    expected: TypeId,
}
#[repr(align(64))]
#[repr(C)]
pub(crate) struct TypelessTask {
    pub(crate) callable: unsafe fn (
        //SELF or NULL
        Option<&'static UnsafeCell<SubScheduler>>,
        //ARGS
        *const [Option<Slot>]),

    pub(crate) drop: unsafe fn(*const ()),

    pub(crate) signature: TypeId,
}

pub const TYPELESS_SIZE: usize = size_of::<TypelessTask>().next_power_of_two();

type Noop = unsafe fn (
    Option<&'static UnsafeCell<SubScheduler>>,
    *const [Option<Slot>]);

impl Default for TypelessTask {
    fn default() -> Self {
        TypelessTask{
            //Garbage data, will be overwritten anyway
            signature: TypeId::of::<i32>(),
            callable: unsafe {transmute(ptr::dangling::<Noop>())},
            drop: |_this| ()
        }
    }
}

pub(crate) trait FromSlot: Sized {
    unsafe fn from_slot(slot: Slot) -> Self;
}

//TODO Eh i dont know bout this
impl<T: 'static> FromSlot for T {
    unsafe fn from_slot(slot: Slot) -> Self {
        assert_eq!(slot.expected, TypeId::of::<T>(), "argument type mismatch");
        *Box::from_raw(slot.raw.as_ptr() as *mut T)
    }
}
