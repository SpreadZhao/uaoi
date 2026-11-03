#[test]
#[ignore = "requires UAOI_TEST_CODEX and UAOI_TEST_PYTHON"]
fn wrapper_process_lifecycle() {
    let status = std::process::Command::new(std::env::var("UAOI_TEST_PYTHON").unwrap())
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/wrapper_fixture.py"
        ))
        .env("UAOI_BINARY", env!("CARGO_BIN_EXE_uaoi"))
        .status()
        .unwrap();
    assert!(status.success());
}
