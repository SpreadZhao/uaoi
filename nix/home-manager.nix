self:
{
  config,
  lib,
  pkgs,
  ...
}:
let
  cfg = config.programs.uaoi;
  format = pkgs.formats.toml { };
in
{
  options.programs.uaoi = {
    enable = lib.mkEnableOption "uaoi";
    package = lib.mkOption {
      type = lib.types.package;
      default = self.packages.${pkgs.stdenv.hostPlatform.system}.uaoi;
      description = "Package providing the uaoi agent bridge.";
    };
    settings = lib.mkOption {
      type = lib.types.nullOr format.type;
      default = null;
      description = "Agent instance configuration. Use token_env for credentials in Nix-generated files.";
    };
  };
  config = lib.mkIf cfg.enable {
    home.packages = [ cfg.package ];
    xdg.configFile."uaoi/config.toml" = lib.mkIf (cfg.settings != null) {
      source = format.generate "uaoi.toml" cfg.settings;
    };
  };
}
