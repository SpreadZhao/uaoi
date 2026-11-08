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
    settings = {
      version = 4;
      agents = { };
      projects = { };
    };
  };
in
assert !(installed.systemd.user.services ? uaoi);
assert configured.xdg.configFile ? "uaoi/config.toml";
assert !(configured.systemd.user.services ? uaoi);
pkgs.runCommand "uaoi-home-manager-check" { } ''
  test -n '${configured.xdg.configFile."uaoi/config.toml".source}'
  touch "$out"
''
