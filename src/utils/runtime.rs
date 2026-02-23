use std::process;

pub fn ensure_posix_or_exit() {
    if !cfg!(unix) {
        eprintln!("unsupported platform: Overhop is intended for POSIX systems");
        process::exit(2);
    }
}
