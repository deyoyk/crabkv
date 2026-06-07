fn main() {
    // Enable SIMD optimizations where available
    println!("cargo:rerun-if-changed=src/");
    
    // Set optimization flags for release builds
    #[cfg(not(debug_assertions))]
    {
        println!("cargo:rustc-link-arg=-Wl,--gc-sections");
    }
}
