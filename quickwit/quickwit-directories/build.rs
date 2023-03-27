fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/hot_directory.proto");
    prost_build::Config::new()
        .out_dir("src/hot_directory")
        .default_package_filename("quickwit_proto")
        .type_attribute(".", "#[derive(Debug, Default)]")
        .compile_protos(&["proto/hot_directory.proto"], &["proto/"])?;
    Ok(())
}
