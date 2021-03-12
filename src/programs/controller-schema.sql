CREATE TABLE IF NOT EXISTS namespace
  ( ns_id         INTEGER PRIMARY KEY --  The namespace ID
  , ns_label      TEXT                -- Namespace name, if given

  , CONSTRAINT ns_label_unique UNIQUE(ns_label)
  );

INSERT INTO namespace(ns_id, ns_label)
  SELECT 0, 'system' WHERE NOT EXISTS(SELECT ns_id FROM namespace WHERE ns_id=0);
INSERT INTO namespace(ns_id, ns_label)
  SELECT 1, 'default' WHERE NOT EXISTS(SELECT ns_id FROM namespace WHERE ns_id=1);

CREATE TABLE IF NOT EXISTS service
  ( s_id          INTEGER NOT NULL -- Service ID. Shifted by 2 ^ 63
  , s_namespace   INTEGER NOT NULL
  , s_label       TEXT

  , s_path        TEXT NOT NULL    -- Path to service directory

  , CONSTRAINT s_label_unique UNIQUE(s_namespace, s_label)
  , CONSTRAINT s_namespace_fk
      FOREIGN KEY (s_namespace)
      REFERENCES  namespace(ns_id)
      ON DELETE CASCADE
  , PRIMARY KEY(s_namespace, s_id)
  );

CREATE TABLE IF NOT EXISTS node
  ( n_id          TEXT PRIMARY KEY     -- Node ID (typically a UUID)
  , n_hostname    TEXT NOT NULL UNIQUE -- Hostname
  , n_ip          TEXT NOT NULL
  , n_state       TEXT NOT NULL

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
  , ps_svc        INTEGER NOT NULL
  , ps_ns         INTEGER NOT NULL

  , ps_state      TEXT NOT NULL
  , ps_placement  TEXT

  , PRIMARY KEY(ps_ns, ps_id)
  , CONSTRAINT ps_service_fk
      FOREIGN KEY (ps_svc, ps_ns)
      REFERENCES service(s_id, s_namespace)
      ON DELETE CASCADE
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

CREATE TABLE IF NOT EXISTS script
  ( scr_id TEXT PRIMARY KEY
  , scr_source TEXT NOT NULL
  );

CREATE TABLE IF NOT EXISTS extension
  ( ext_id       TEXT PRIMARY KEY
  , ext_version  INTEGER NOT NULL
  , ext_src_path TEXT NOT NULL
  );
