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
      description = "Agent instance configuration. Reference credentials by environment variable name.";
    };
    service.enable = lib.mkEnableOption "the persistent uaoi user service";
    service.environmentFile = lib.mkOption {
      type = lib.types.nullOr lib.types.str;
      default = null;
      description = "External environment file containing bot credentials and launcher environment.";
    };
  };
  config = lib.mkIf cfg.enable {
    home.packages = [ cfg.package ];
    xdg.configFile."uaoi/config.toml" = lib.mkIf (cfg.settings != null) {
      source = format.generate "uaoi.toml" cfg.settings;
    };
    systemd.user.services.uaoi = lib.mkIf cfg.service.enable {
      Unit = {
        Description = "uaoi Telegram agent bridge";
        After = [ "network-online.target" ];
      };
      Service = {
        ExecStart = "${cfg.package}/bin/uaoi --config ${config.xdg.configHome}/uaoi/config.toml serve";
        Restart = "on-failure";
        RestartSec = 3;
      }
      // lib.optionalAttrs (cfg.service.environmentFile != null) {
        EnvironmentFile = cfg.service.environmentFile;
      };
      Install.WantedBy = [ "default.target" ];
    };
  };
}
