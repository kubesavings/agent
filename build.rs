fn main() {
    prost_build::Config::new()
        .out_dir(std::env::var("OUT_DIR").unwrap())
        .compile_protos(&["proto/kubesavings.proto"], &["proto/"])
        .expect("failed to compile protobuf schema");
}
