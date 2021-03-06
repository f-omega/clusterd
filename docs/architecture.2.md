# Architecture

Clusterd associates namespaces with particular nix store derivations
that contain the services for that namespace.

To start a namespace, you publish the built-derivation as a torrent
and send a command to the leader to download the torrent, check the
description, etc.

The namespace derivation does not link to all the images, because that
would cause unnecessary network usage.

# Tables

CREATE TABLE namespace
 ( nsid INTEGER PRIMARY KEY,
   created TIMESTAMP NOT NULL,
   latest_revision TEXT NOT NULL ); -- Nix store path of cluster definition
