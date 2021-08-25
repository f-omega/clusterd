CREATE TABLE IF NOT EXISTS namespace
  ( ns_id         INTEGER PRIMARY KEY --  The namespace ID
  , ns_label      TEXT                -- Namespace name, if given

  , CONSTRAINT ns_label_unique UNIQUE(ns_label)
  );

INSERT INTO namespace(ns_id, ns_label)
  SELECT 0, 'system' WHERE NOT EXISTS(SELECT ns_id FROM namespace WHERE ns_id=0);
INSERT INTO namespace(ns_id, ns_label)
  SELECT 1, 'default' WHERE NOT EXISTS(SELECT ns_id FROM namespace WHERE ns_id=1);

CREATE TABLE IF NOT EXISTS node
  ( n_id          TEXT PRIMARY KEY     -- Node ID (typically a UUID)
  , n_hostname    TEXT NOT NULL UNIQUE -- Hostname
  , n_ip          TEXT NOT NULL
  , n_state       TEXT NOT NULL

  , n_monitor_avail BOOLEAN NOT NULL DEFAULT true -- Whether or not monitoring is available on this host

  , CONSTRAINT n_state_enum
      CHECK (n_state IN ('up', 'down'))
  );

CREATE TABLE IF NOT EXISTS failure_domain
  ( fd_name        TEXT PRIMARY KEY
  , fd_description TEXT
  , fd_parent      TEXT

  , CONSTRAINT fd_parent_fk
      FOREIGN KEY (fd_parent)
      REFERENCES failure_domain(fd_name)
      ON DELETE CASCADE
  , CONSTRAINT fd_one_child
      UNIQUE(fd_parent)
  );

CREATE TABLE IF NOT EXISTS node_failure_domain
  ( nfd_node       TEXT NOT NULL
  , nfd_name       TEXT NOT NULL
  , nfd_value      TEXT NOT NULL

  , PRIMARY KEY (nfd_node, nfd_name)
  , CONSTRAINT nfd_node_fk
      FOREIGN KEY (nfd_node)
      REFERENCES node(n_id)
      ON DELETE CASCADE
  , CONSTRAINT nfd_name_fk
      FOREIGN KEY (nfd_name)
      REFERENCES failure_domain(fd_name)
      ON DELETE CASCADE
  );

CREATE TABLE IF NOT EXISTS resource_class
  ( rc_name        TEXT PRIMARY KEY
  , rc_fungible    BOOLEAN NOT NULL     -- Fungible resources are respected while scheduling, but the
                                        -- exact resources in use are not expilicitly tracked.
  , rc_quantifiable BOOLEAN NOT NULL    -- If false, then the resource has no quantity
  , rc_description TEXT

  , rc_parent      TEXT

  , CONSTRAINT rc_parent_fk
      FOREIGN KEY (rc_parent)
      REFERENCES resource_class(rc_name)
      ON DELETE SET NULL
  );

CREATE TABLE IF NOT EXISTS node_resource
  ( nrc_name      TEXT NOT NULL
  , nrc_node      TEXT NOT NULL
  , nrc_amount    INTEGER        -- The amount of this resource, if any

  , CONSTRAINT nrc_name_fk
      FOREIGN KEY (nrc_name)
      REFERENCES resource_class(rc_name)
      ON DELETE CASCADE
  , CONSTRAINT nrc_node_fk
      FOREIGN KEY (nrc_node)
      REFERENCES node(n_id)
      ON DELETE CASCADE
  , PRIMARY KEY (nrc_node, nrc_name)
  );

CREATE TABLE IF NOT EXISTS process
  ( ps_id         INTEGER NOT NULL
  , ps_image      TEXT NOT NULL -- Nix path to process image
  , ps_ns         INTEGER NOT NULL

  , ps_state      TEXT NOT NULL
  , ps_placement  TEXT

  , PRIMARY KEY(ps_ns, ps_id)
  , CONSTRAINT ps_state_enum
      CHECK (ps_state in ('scheduling', 'starting', 'up', 'down', 'zombie'))
  );

CREATE TRIGGER IF NOT EXISTS ps_state_on_node_delete
  AFTER DELETE ON node
BEGIN
  UPDATE process
    SET ps_state='down'
    WHERE ps_placement=OLD.n_id;
END;

CREATE TRIGGER IF NOT EXISTS ps_state_on_node_down
  AFTER DELETE ON node
BEGIN
  UPDATE process
    SET ps_state='down'
    WHERE ps_placement=OLD.n_id
      AND OLD.n_state='up'
      AND NEW.n_state='down';
END;

-- Global resources are things that exist globally in the cluster, in
-- some namespace, and on some set of nodes.

CREATE TABLE IF NOT EXISTS global_resource
  ( gr_ns INTEGER NOT NULL

  , gr_name       TEXT NOT NULL
  , gr_management_process INTEGER NOT NULL

  -- JSON description of this resource
  , gr_metadata   TEXT NOT NULL
  -- Some representation of the type of this resource
  , gr_type       TEXT NOT NULL

  -- Human readable description of the resource
  , gr_description TEXT

  -- If true, then the global resource should automatically be deleted
  -- when no more processes are using it
  , gr_persistent BOOLEAN NOT NULL

  -- If true, then the global resource is available for
  -- use. Otherwise, attempting to attach it to a process will fail.
  , gr_available BOOLEAN NOT NULL

  , PRIMARY KEY (gr_ns, gr_name)
  , CONSTRAINT gr_namespace_fk
      FOREIGN KEY (gr_ns)
      REFERENCES namespace(ns_id)
      ON DELETE RESTRICT
  , CONSTRAINT gr_management_process_fk
      FOREIGN KEY (gr_ns, gr_management_process)
      REFERENCES process(ps_ns, ps_id)
      ON DELETE RESTRICT
  );

-- This table maps global resources to the nodes that host them
CREATE TABLE IF NOT EXISTS global_resource_assignment
  ( gra_ns INTEGER NOT NULL
  , gra_resource  TEXT NOT NULL
  , gra_node      TEXT NOT NULL

  , gra_rel       TEXT NOT NULL
  , gra_description TEXT -- Human readable version of rel
  , gra_metadata  TEXT NOT NULL -- JSON representation of any metadat

  -- If true, then any process that needs to use this resource, will
  -- have to be scheduled on this node
  , gra_enforce_affinity BOOLEAN NOT NULL

  , PRIMARY KEY (gra_ns, gra_resource, gra_rel, gra_node)
  , CONSTRAINT gra_namespace_fk
      FOREIGN KEY (gra_ns)
      REFERENCES namespace(ns_id)
      ON DELETE RESTRICT
  , CONSTRAINT gra_resource_fk
      FOREIGN KEY (gra_ns, gra_resource)
      REFERENCES global_resource(gr_ns, gr_name)
      ON DELETE CASCADE
  , CONSTRAINT gra_node_fk
      FOREIGN KEY (gra_node)
      REFERENCES node(n_id)
      ON DELETE RESTRICT
  );

-- This makes sure that at most one node is set as the one with affinity
CREATE UNIQUE INDEX IF NOT EXISTS global_resource_assignment_affinity
  ON global_resource_assignment (gra_ns, gra_resource)
  WHERE gra_enforce_affinity;

CREATE TABLE IF NOT EXISTS global_resource_claim
  ( grc_ns INTEGER NOT NULL
  , grc_resource TEXT NOT NULL
  , grc_process  INTEGER NOT NULL

  , PRIMARY KEY (grc_ns, grc_resource, grc_process)
  , CONSTRAINT gra_process_fk
      FOREIGN KEY (grc_ns, grc_process)
      REFERENCES process(ps_ns, ps_id)
      ON DELETE CASCADE
  );

-- An endpoint is a static IP that gets resolved by the default
-- clusterd resolver to an internal IPv6 address that can load balance
-- a connection between multiple target processes.

CREATE TABLE IF NOT EXISTS endpoint
  ( ep_ns INTEGER NOT NULL

  , ep_id INTEGER NOT NULL

  , PRIMARY KEY (ep_ns, ep_id)

  , CONSTRAINT ep_namespace_fk
      FOREIGN KEY (ep_ns)
      REFERENCES namespace(ns_id)
      ON DELETE CASCADE
  );

CREATE TABLE IF NOT EXISTS endpoint_claim
  ( epc_ns INTEGER NOT NULL
  , epc_id INTEGER NOT NULL
  , epc_process INTEGER NOT NULL

  , PRIMARY KEY (epc_ns, epc_id, epc_process)
  , CONSTRAINT epc_endpoint_fk
      FOREIGN KEY (epc_ns, epc_id)
      REFERENCES endpoint(ep_ns, ep_id)
      ON DELETE CASCADE
  , CONSTRAINT epc_process_fk
      FOREIGN KEY (epc_ns, epc_process)
      REFERENCES process(ps_ns, ps_id)
      ON DELETE CASCADE
  );

CREATE TABLE IF NOT EXISTS endpoint_name
  ( epn_ns INTEGER NOT NULL
  , epn_id INTEGER NOT NULL
  , epn_name TEXT NOT NULL

  , PRIMARY KEY (epn_ns, epn_name)
  , CONSTRAINT epn_endpoint_fk
      FOREIGN KEY (epn_ns, epn_id)
      REFERENCES endpoint(ep_ns, ep_id)
      ON DELETE CASCADE
  );

-- Signals are enqueued messages that are sent to specific
-- processes. Signals are delivered to a process by sending a signal
-- using the same technique as clusterd-kill (send a message to the
-- host's monitor to kill the process).
--
-- When a signal has successfully been flagged, a bit is set, but the
-- signal is not removed. A signal is removed when a process calls
-- clusterd.set_signal_mark, which sets the id of the last signal
-- received.

CREATE TABLE IF NOT EXISTS enqueued_signal
  ( enqsig_ns INTEGER NOT NULL
  , enqsig_ps INTEGER NOT NULL

  , enqsig_pos INTEGER NOT NULL

  , enqsig_signal TEXT NOT NULL

  -- Set to true if the clusterd-kill operation has succeeded after
  -- this signal was enqueued.
  , enqsig_flagged BOOLEAN NOT NULL

  , PRIMARY KEY (enqsig_ns, enqsig_ps, enqsig_pos)
  , CONSTRAINT enqsig_process_fk
      FOREIGN KEY (enqsig_ns, enqsig_ps)
      REFERENCES process(ps_ns, ps_id)
      ON DELETE CASCADE
  );

CREATE TABLE IF NOT EXISTS process_sigmask
  ( pssig_ns INTEGER NOT NULL
  , pssig_ps INTEGER NOT NULL
  , pssig_id INTEGER NOT NULL

  , pssig_mask TEXT NOT NULL

  , PRIMARY KEY (pssig_ns, pssig_ps, pssig_id)

  , CONSTRAINT pssig_process_fk
      FOREIGN KEY (pssig_ns, pssig_ps)
      REFERENCES process(ps_ns, ps_id)
      ON DELETE CASCADE
  );

CREATE TABLE IF NOT EXISTS process_monitor
  ( psmon_ns INTEGER NOT NULL
  , psmon_ps INTEGER NOT NULL
  , psmon_node TEXT NOT NULL

  , CONSTRAINT psmon_node_fk
      FOREIGN KEY (psmon_node)
      REFERENCES node(n_id)
      ON DELETE CASCADE

  , CONSTRAINT psmon_process_fk
      FOREIGN KEY (psmon_ns, psmon_ps)
      REFERENCES process(ps_ns, ps_id)
      ON DELETE CASCADE
  );
