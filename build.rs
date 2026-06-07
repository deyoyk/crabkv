fn main() {
    println!("cargo:rerun-if-changed=src/");

    #[cfg(not(debug_assertions))]
    {
        let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
        match target_os.as_str() {
            "linux" | "android" => {
                println!("cargo:rustc-link-arg=-Wl,--gc-sections");
            }
            "macos" | "ios" => {
                println!("cargo:rustc-link-arg=-Wl,-dead_strip");
            }
            _ => {}
        }
    }
}
