pub fn print_startup_banner() {
    const RESET: &str = "\x1b[0m";
    const BANNER_COLOR: &str = "\x1b[38;5;66m";
    const DIM_GRAY: &str = "\x1b[2;90m";
    const BANNER: &str = r#"
                                         █████                         ███
                                        ░░███                         ░███
  ██████  █████ █████  ██████  ████████  ░███████    ██████  ████████ ░███
 ███░░███░░███ ░░███  ███░░███░░███░░███ ░███░░███  ███░░███░░███░░███░███
░███ ░███ ░███  ░███ ░███████  ░███ ░░░  ░███ ░███ ░███ ░███ ░███ ░███░███
░███ ░███ ░░███ ███  ░███░░░   ░███      ░███ ░███ ░███ ░███ ░███ ░███░░░
░░██████   ░░█████   ░░██████  █████     ████ █████░░██████  ░███████  ███
 ░░░░░░     ░░░░░     ░░░░░░  ░░░░░     ░░░░ ░░░░░  ░░░░░░   ░███░░░  ░░░
                                                             ░███
                                                             █████
                                                            ░░░░░         "#;
    const APP_DESCRIPTION: &str =
        "Persistent queue orchestration and worker coordination runtime over TCP.";
    const REPO_URL: &str = "https://github.com/mwognicki/overhop";
    const COPYRIGHT_NOTICE: &str = "Copyright (c) 2026 Marek Kapusta-Ognicki";
    const LIABILITY_NOTICE: &str =
        "MIT License disclaimer: software is provided \"AS IS\", without warranty or liability.";

    println!("{BANNER_COLOR}");
    println!("{BANNER}{RESET}");
    println!(
        "{} v{} | build {}",
        env!("CARGO_PKG_NAME"),
        env!("CARGO_PKG_VERSION"),
        env!("OVERHOP_BUILD_DATE_UTC")
    );
    println!("{APP_DESCRIPTION}");
    println!("Repository: {REPO_URL}");
    println!("{DIM_GRAY}{COPYRIGHT_NOTICE}{RESET}");
    println!("{DIM_GRAY}{LIABILITY_NOTICE}{RESET}");
    println!();
    println!("================================================================");
    println!();
}
