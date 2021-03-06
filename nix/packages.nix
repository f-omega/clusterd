let overlay = self: super: {
      raft-canonical = super.raft-canonical.overrideDerivation (old: rec {
        name = "raft-canonical-${version}";
        version = "0.9.25";
        src = self.fetchFromGitHub {
          owner = "canonical";
          repo = "raft";
          rev = "v0.9.25";
          sha256 = "1y6ksl9ijsmflghirdbcfafblzngv1gb1w86mriph4kl13733cfz";
        };
      });
    };

in import <nixpkgs> { overlays = [ overlay ]; }

