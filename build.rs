fn main() {
    tonic_build::configure()
        .type_attribute(
            "build.bazel.remote.execution.v2.Digest",
            "#[derive(Eq, Hash)]",
        )
        .type_attribute(
            "build.bazel.remote.asset.v1.Qualifier",
            "#[derive(PartialOrd, Ord, Eq, Hash)]",
        )
        .compile(
            &[
                "remote-apis/build/bazel/remote/execution/v2/remote_execution.proto",
                "remote-apis/build/bazel/remote/asset/v1/remote_asset.proto",
                "googleapis/google/bytestream/bytestream.proto",
                "googleapis/google/rpc/code.proto",
            ],
            &["remote-apis", "googleapis"],
        )
        .unwrap();
}
