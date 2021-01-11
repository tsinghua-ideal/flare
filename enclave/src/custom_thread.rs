// Port from: https://github.com/rust-lang/rust/tree/master/library/std/src/sys/unix/thread.rs
use sgx_trts::c_str;

use std::boxed::Box;
use std::cmp;
use std::io;
use std::mem;
use std::ptr;
use std::time::Duration;

pub struct PThread {
    id: libc::pthread_t,
}

unsafe impl Send for PThread {}
unsafe impl Sync for PThread {}

impl PThread {
    // unsafe: see thread::Builder::spawn_unchecked for safety requirements
    pub unsafe fn new(p: Box<dyn FnOnce()>) -> io::Result<PThread> {
        let p = Box::into_raw(box p);
        let mut native: libc::pthread_t = mem::zeroed();
        let mut attr: libc::pthread_attr_t = mem::zeroed();

        let ret = libc::pthread_create(&mut native, &attr, thread_start, p as *mut _);

        return if ret != 0 {
            // The thread failed to start and as a result p was not consumed. Therefore, it is
            // safe to reconstruct the box so that it gets deallocated.
            mem::drop(Box::from_raw(p));
            Err(io::Error::from_raw_os_error(ret))
        } else {
            Ok(PThread { id: native })
        };

        extern "C" fn thread_start(main: *mut libc::c_void) -> *mut libc::c_void {
            unsafe {
                /*
                // Next, set up our stack overflow handler which may get triggered if we run
                // out of stack.
                let _handler = stack_overflow::Handler::new();
                */
                // Finally, let's run some code.
                Box::from_raw(main as *mut Box<dyn FnOnce()>)();
            }
            ptr::null_mut()
        }
    }

    pub fn yield_now() {
        let ret = unsafe { libc::ocall::sched_yield() };
        debug_assert_eq!(ret, 0);
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn set_name(name: &c_str::CStr) {
        const PR_SET_NAME: libc::c_int = 15;
        // pthread wrapper only appeared in glibc 2.12, so we use syscall
        // directly.
        unsafe {
            libc::ocall::prctl(PR_SET_NAME, name.as_ptr() as libc::c_ulong, 0, 0, 0);
        }
    }

    pub fn sleep(dur: Duration) {
        let mut secs = dur.as_secs();
        let mut nsecs = dur.subsec_nanos() as _;

        // If we're awoken with a signal then the return value will be -1 and
        // nanosleep will fill in `ts` with the remaining time.
        unsafe {
            while secs > 0 || nsecs > 0 {
                let mut ts = libc::timespec {
                    tv_sec: cmp::min(libc::time_t::MAX as u64, secs) as libc::time_t,
                    tv_nsec: nsecs,
                };
                secs -= ts.tv_sec as u64;
                let ts_ptr = &mut ts as *mut _;
                if libc::ocall::nanosleep(ts_ptr, ts_ptr) == -1 {
                    assert_eq!(libc::errno(), libc::EINTR);
                    secs += ts.tv_sec as u64;
                    nsecs = ts.tv_nsec;
                } else {
                    nsecs = 0;
                }
            }
        }
    }

    pub fn join(self) {
        unsafe {
            let ret = libc::pthread_join(self.id, ptr::null_mut());
            mem::forget(self);
            assert!(ret == 0, "failed to join thread: {}", io::Error::from_raw_os_error(ret));
        }
    }

    pub fn id(&self) -> libc::pthread_t {
        self.id
    }

    pub fn into_id(self) -> libc::pthread_t {
        let id = self.id;
        mem::forget(self);
        id
    }
}