{
  self,
  home-manager,
  nixpkgs,
  system,
}:
let
  pkgs = nixpkgs.legacyPackages.${system};
  evaluate =
    settings:
    (home-manager.lib.homeManagerConfiguration {
      inherit pkgs;
      modules = [
        self.homeManagerModules.uaoi
        {
          home.username = "tester";
          home.homeDirectory = "/home/tester";
          home.stateVersion = "25.11";
          programs.uaoi = settings;
        }
      ];
    }).config;
  installed = evaluate { enable = true; };
  configured = evaluate {
    enable = true;
    settings.projects = { };
  };
in
assert !(installed.systemd.user.services ? uaoi);
assert configured.xdg.configFile ? "uaoi/config.toml";
pkgs.runCommand "uaoi-home-manager-check" { } ''
  test -n '${configured.xdg.configFile."uaoi/config.toml".source}'
  touch "$out"
''
