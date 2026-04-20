fn main() {
    println!("cargo:rerun-if-changed=src/serializers/siscom.proto");

    let protoc = protoc_bin_vendored::protoc_bin_path().expect("failed to locate protoc binary");
    std::env::set_var("PROTOC", protoc);

    prost_build::compile_protos(&["src/serializers/siscom.proto"], &["src/serializers"])
        .expect("failed to compile protobuf schema");
}
