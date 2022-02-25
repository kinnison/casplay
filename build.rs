fn main() {
    tonic_build::configure()
        .compile(
            &[
                "remote-apis/build/bazel/remote/execution/v2/remote_execution.proto",
                "googleapis/google/bytestream/bytestream.proto",
                "googleapis/google/rpc/code.proto",
            ],
            &["remote-apis", "googleapis"],
        )
        .unwrap();
}
