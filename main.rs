
use crate::scheduler::SelfHandle;
use crate::scheduler_config::Config;
use crate::task::AnyArg;
use std::any::Any;
mod scheduler;
mod scheduler_config;
mod builder;
mod task;
mod worker;


fn is_even(val: u64) -> bool{
    val & 1 == 1
}
fn is_even2(val: u64) -> (){

}

fn is_something(val: u64, other: String) -> bool{
    val & 2 == 2
}

fn empty1(_: ()) ->(){}
fn empty2() ->(){}


struct OwnershipTest{}
impl OwnershipTest{
    fn new() -> Self {
        Self{}
    }
}

#[tokio::main]
async fn main() {
    

    let x = (1,"",3,6u16,5);
    let mut sh = builder::Scheduler::new(Config::default())
        .add_scheduler::<i32>()
        .add_scheduler::<u8>()
        .add_scheduler::<(u64, String)>()
        .apply();
    //let mut sh = add_and_impl_schedulers!(sh, u64, i32, (u64, String)).apply();


    let on = OwnershipTest::new();

    let self_handle1 = unsafe { SelfHandle::new(&on) };
    let self_handle2 = unsafe { SelfHandle::new(&on) };

    sh.any(self_handle1, vec![Box::new(67u64)], is_even2).await;
    sh.any(self_handle2, vec![Box::new(0u64), Box::new(String::from("test"))], is_something).await;
}
