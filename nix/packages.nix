let overlay = self: super: {
      raft-canonical = super.raft-canonical.overrideDerivation (old: rec {
        name = "raft-canonical-${version}";
        version = "0.10.0";
        src = self.fetchFromGitHub {
          owner = "canonical";
          repo = "raft";
          rev = "v0.10.0";
          sha256 = "Mr/m03/q2p8p4L9IVCtxvwQ8dijqbA/R8Io/Vq5TAqc=";
        };
      });
    };

in import <nixpkgs> { overlays = [ overlay ]; }

