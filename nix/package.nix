{
  lib,
  rustPlatform,
  python3,
}:
rustPlatform.buildRustPackage {
  pname = "uaoi";
  version = "0.1.0";
  src = lib.fileset.toSource {
    root = ../.;
    fileset = lib.fileset.unions [
      ../Cargo.toml
      ../Cargo.lock
      ../src
      ../tests
    ];
  };
  nativeCheckInputs = [ python3 ];
  cargoLock.lockFile = ../Cargo.lock;
  meta = {
    description = "Control coding agents from IM and their native terminal";
    mainProgram = "uaoi";
    license = lib.licenses.mit;
    platforms = lib.platforms.linux;
  };
}
