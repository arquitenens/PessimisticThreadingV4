use std::any::Any;

pub struct Config{
    pub(crate) max_threads: usize,
    pub(crate) threads_per_worker: usize,
}
impl Config{
    pub fn new(max_threads: usize, threads_per_worker: usize) -> Self{
        assert!(max_threads > 0, "max_threads must be > 0");
        assert!(threads_per_worker > 0, "threads_per_worker must be > 0");
        assert!(threads_per_worker <= 64, "threads_per_worker must be < 64");
        Config{max_threads, threads_per_worker }
    }
}

impl Default for Config{
    fn default() -> Self{
        Config{max_threads: 8 ,threads_per_worker: 4}
    }
}


pub(crate) trait AnyHandler<Args> {
    type Output;
    fn call_any(self, args: Vec<Box<dyn Any>>) -> Option<Self::Output>;

    fn matching_arguments(&self, other: &[Box<dyn Any>]) -> bool;
}

#[macro_export]
macro_rules! impl_any_handler {
    ($($T:ident),*) => {
        #[allow(non_snake_case, unused_mut, unused_variables)]
        impl<Func, R, $($T,)*> AnyHandler<($($T,)*)> for Func
        where
            Func: FnOnce($($T),*) -> R,
            $($T: Any + 'static,)*
        {
            type Output = R;
            fn call_any(self, args: Vec<Box<dyn Any>>) -> Option<R> {
                let mut iter = args.into_iter();
                $(
                    let $T = *iter.next()?.downcast::<$T>().ok()?;
                )*
                Some(self($($T),*))
            }
            fn matching_arguments(&self, args: &[Box<dyn Any>]) -> bool {
                let expected: Vec<TypeId> = vec![$(TypeId::of::<$T>()),*];
                if expected.len() != args.len() {
                    return false;
                }
                args.iter()
                    .zip(expected.iter())
                    .all(|(a, t)| {
                    let actual = (**a).type_id();
                    actual == *t
                })
            }
        }
    };
}


