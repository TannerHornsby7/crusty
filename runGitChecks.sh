cargo check --all --all-targets --verbose --no-default-features && 
cargo fmt --all -- --check &&
cargo clippy --all --all-targets --verbose --no-default-features &&
cargo clippy --all --all-targets --verbose --all-features &&
echo "All good!"
