// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

fn main() -> Result<(), String> {
    use std::io::Write;

    let in_dir = std::path::PathBuf::from("src/proto/src");
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    let destination_dir = std::path::PathBuf::from("src/proto/generated");

    // for use in docker build where file changes can be wonky
    println!("cargo:rerun-if-env-changed=FORCE_REBUILD");

    let version = rustc_version::version().unwrap();
    println!("cargo:rustc-env=RUSTC_VERSION={version}");

    // Input file relative to in_dir, output file relative to out_dir, destination file relative to destination_dir
    let proto_files = [
        ("datafusion_ray.proto", "datafusion_ray.protobuf.rs", "protobuf.rs"),
        ("streaming.proto", "datafusion_ray.protobuf.streaming.rs", "streaming.rs"),
    ];

    // We don't include the proto files in releases so that downstreams
    // do not need to have PROTOC included
    if in_dir.join(proto_files[0].0).exists() {
        println!("cargo:rerun-if-changed=src/proto/src/datafusion_common.proto");
        println!("cargo:rerun-if-changed=src/proto/src/datafusion.proto");
        println!("cargo:rerun-if-changed=src/proto/src/datafusion_ray.proto");
        for (file, _, _) in proto_files {
            println!("cargo:rerun-if-changed={}", in_dir.join(file).display());
        }

        // Other dependencies
        for file in [
            "datafusion_common.proto",
            "datafusion.proto",
        ] {
            println!("cargo:rerun-if-changed={}", in_dir.join(file).display());
        }

        let input_files: Vec<_> = proto_files.iter()
            .map(|(input_file, _, _)| in_dir.join(input_file))
            .collect();
        tonic_build::configure()
            .extern_path(".datafusion", "::datafusion_proto::protobuf")
            .extern_path(".datafusion_common", "::datafusion_proto::protobuf")
            .compile(&input_files, &[in_dir])
            .map_err(|e| format!("protobuf compilation failed: {e}"))?;

        for (_, output_file, destination_file) in proto_files.iter() {
            let output_source_path = out_dir.join(output_file);
            let code = std::fs::read_to_string(output_source_path).unwrap();
            let mut file = std::fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .create(true)
                .open(destination_dir.join(destination_file))
                .unwrap();
            file.write_all(code.as_str().as_ref()).unwrap();
        }
    }

    Ok(())
}
