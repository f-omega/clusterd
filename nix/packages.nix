let overlay = self: super: {
      raft-canonical = super.raft-canonical.overrideDerivation (old: rec {
        name = "raft-canonical-${version}";
        version = "0.10.0";
        src = self.fetchFromGitHub {
          owner = "f-omega";
          repo = "raft";
          rev = "3fe15a8f06fe13eb7d53d7b0b416fa7bec3680ef";
          sha256 = "bv745luT5Zj6dKobjG1s9Kc5V8zCwDwV/oI7x4AUETg=";
        };

        buildInputs = old.buildInputs ++ [ self.lz4.dev ];
      });
    };

in import <nixpkgs> { overlays = [ overlay ]; }

