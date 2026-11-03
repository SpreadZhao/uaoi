{
  description = "uaoi - Use Agent On IM";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    naw.url = "github:SpreadZhao/NAW";
    naw.inputs.nixpkgs.follows = "nixpkgs";
    home-manager.url = "github:nix-community/home-manager";
    home-manager.inputs.nixpkgs.follows = "nixpkgs";
  };
  outputs =
    {
      self,
      nixpkgs,
      naw,
      home-manager,
      ...
    }:
    let
      systems = [
        "x86_64-linux"
        "aarch64-linux"
      ];
      each = nixpkgs.lib.genAttrs systems;
      workspace = naw.lib.mkWorkspace {
        inherit systems;
        targets = [
          "agents"
          "claude"
        ];
        instructions = ''
          # uaoi development
          uaoi is a Rust CLI wrapper with an on-demand broker, IM and agent adapters.
          Keep platform identifiers and native agent protocols in their adapters.
          Keep documentation, comments and CLI help in English.
          Maintain instructions here in flake.nix; NAW generates AGENTS.md.
          Never run NixOS or Home Manager switch commands.
          Check cargo fmt, cargo clippy, cargo test and nix flake check.
          Keep credentials, runtime state and generated NAW files out of Git.
        '';
        packages =
          pkgs: with pkgs; [
            cargo
            rustc
            rustfmt
            clippy
            rust-analyzer
            python3
            nixfmt
            git
            ripgrep
            pkg-config
          ];
      };
    in
    {
      inherit (workspace) devShells;
      packages = each (
        system:
        let
          pkgs = import nixpkgs { inherit system; };
          package = pkgs.callPackage ./nix/package.nix { };
        in
        {
          default = package;
          uaoi = package;
        }
      );
      apps = each (
        system:
        let
          app = {
            type = "app";
            program = "${self.packages.${system}.uaoi}/bin/uaoi";
            meta.description = "Use Agent On IM";
          };
        in
        {
          default = app;
          uaoi = app;
        }
      );
      homeManagerModules = {
        default = self.homeManagerModules.uaoi;
        uaoi = import ./nix/home-manager.nix self;
      };
      checks = each (
        system:
        workspace.checks.${system}
        // {
          package = self.packages.${system}.uaoi;
          home-manager = import ./nix/module-check.nix {
            inherit
              self
              home-manager
              nixpkgs
              system
              ;
          };
        }
      );
      formatter = each (system: nixpkgs.legacyPackages.${system}.nixfmt);
    };
}
