rm -rf output && mkdir output
cargo run --manifest-path tool/Cargo.toml index data output/directory.db
cargo run --manifest-path tool/Cargo.toml render output/directory.db templates output/html

