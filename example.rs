struct Fetch{}
struct Post{}

//Sadly you have to do the indexing yourself, its simply but avoids global counters that would require more overhead
//Just for every new type you add increment it by 1
impl TypesIdx for Fetch {
    fn tid_idx() -> usize {
        0
    }
}
impl TypesIdx for Post {
    fn tid_idx() -> usize {
        1
    }
}

//You should use functions because normal inline closures are unique and dont hit the caching
fn counter_task1(x: Arc<AtomicU64>) -> impl FnMut() + Send + 'static {
    move || {
        x.fetch_add(1, Ordering::Release);
    }
}

fn counter_task2(x: Arc<AtomicU64>) -> impl FnMut() + Send + 'static {
    move || {
        x.fetch_add(1, Ordering::Release);
    }
}


fn main() {
    let config = Config::new(8, 8);


    //IMPORTANT due to some limitations the scheduler dropping frees ANY task
    //Which means in the counter example you might get 1 or 2 less than expected, either you don't care about that
    //Or you extend the lifetime of the scheduler until all tasks are completed

    let mut sh = Scheduler::new(config)
        //the Fetch and Post structs are just identifiers, and it separates the threads
        .add_scheduler::<Fetch>(ThreadAmount::Default)
        .add_scheduler::<Post>(ThreadAmount::Default)
        .apply();

    let counter = Arc::new(AtomicU64::new(0));

    let now = Instant::now();
    for _ in 0..5_000_000 {

        //if the workers are occupied simply drops the work
        let _result_once = sh.any::<Post, _>(counter_task2(counter.clone()));


        let result_retry = sh.any::<Fetch, _>(counter_task1(counter.clone()));

        //Keeps retrying the send, only matters if all workers are occupied so it spins to grab the first available one
        match result_retry {
            Ok(()) => {},
            Err(WorkerErrors::Busy(task)) => {
                let mut temp = task;
                while let Err(WorkerErrors::Busy(t)) = sh.any::<Fetch, _>(temp) {
                    temp = t;
                }
            }
            //I didnt add any other errors, too lazy!
            Err(WorkerErrors::Misc) => {}
        }

    }
    let elapsed = now.elapsed();
    println!("Elapsed: {:?}", elapsed);


}
