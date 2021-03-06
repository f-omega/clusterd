{ pkgs ? import ./nix/packages.nix }:

with pkgs;

stdenv.mkDerivation {
  name = "clusterd";

  buildInputs = [ cmake libuv pkg-config ronn raft-canonical lua5_3 gdb valgrind sqlite debianutils ];
}
