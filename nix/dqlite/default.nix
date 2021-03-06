{ pkgs, lib, stdenv, autoreconfHook, pkgconfig, sqlite, libuv, raft-canonical, fetchFromGitHub }:

stdenv.mkDerivation {
  name = "dqlite";
  version = "1.6.0";

  src = fetchFromGitHub {
    owner = "canonical";
    repo = "dqlite";
    rev = "v1.6.0";
    sha256 = "0gkmzvrc6dw0xvr3fqkmh90i2cv45smprj9xvj8pm82vbzvz5as9";
  };

  buildInputs = [ autoreconfHook pkgconfig sqlite libuv ];

  meta = {
    description = "Embeddable, replicated and fault tolerant SQL engine.";
    homepage = http://dqlite.io;
    license = lib.licenses.lgpl3;
    platforms = lib.platforms.all;
  };
}
