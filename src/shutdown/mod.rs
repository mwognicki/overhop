use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

#[cfg(unix)]
use signal_hook::consts::signal::{SIGINT, SIGTERM};
#[cfg(unix)]
use signal_hook::flag;
#[cfg(unix)]
use signal_hook::low_level::unregister;
#[cfg(unix)]
use signal_hook::SigId;

pub struct ShutdownHooks {
    triggered: Arc<AtomicBool>,
    #[cfg(unix)]
    sig_ids: Vec<SigId>,
}

impl ShutdownHooks {
    pub fn install() -> io::Result<Self> {
        let triggered = Arc::new(AtomicBool::new(false));

        #[cfg(unix)]
        {
            let id_int = flag::register(SIGINT, Arc::clone(&triggered))?;
            let id_term = flag::register(SIGTERM, Arc::clone(&triggered))?;
            return Ok(Self {
                triggered,
                sig_ids: vec![id_int, id_term],
            });
        }

        #[cfg(not(unix))]
        {
            Ok(Self { triggered })
        }
    }

    pub fn is_triggered(&self) -> bool {
        self.triggered.load(Ordering::SeqCst)
    }
}

impl Drop for ShutdownHooks {
    fn drop(&mut self) {
        #[cfg(unix)]
        for id in self.sig_ids.drain(..) {
            unregister(id);
        }
    }
}
