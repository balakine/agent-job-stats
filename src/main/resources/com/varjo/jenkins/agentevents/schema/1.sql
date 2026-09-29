-- The initial schema.

CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);

-- The epoch names this database's numbering. It changes only if the file is
-- recreated, which is the one way a cursor can stop meaning anything.
INSERT INTO meta (key, value) VALUES ('epoch', lower(hex(randomblob(16))));

-- One row: the last sequence number handed out. Stored rather than derived from
-- the maximum in use, so pruning old rows can never make a number be issued twice.
CREATE TABLE sequence (last INTEGER NOT NULL);
INSERT INTO sequence (last) VALUES (0);

CREATE TABLE jobs (
    id TEXT PRIMARY KEY,
    full_name TEXT NOT NULL,
    deleted_at INTEGER
);
-- A name belongs to at most one live job; deleted ones keep theirs.
CREATE UNIQUE INDEX jobs_live_name ON jobs (full_name) WHERE deleted_at IS NULL;

CREATE TABLE nodes (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    deleted_at INTEGER
);
CREATE UNIQUE INDEX nodes_live_name ON nodes (name) WHERE deleted_at IS NULL;

CREATE TABLE builds (
    id INTEGER PRIMARY KEY,
    job_id TEXT NOT NULL REFERENCES jobs (id),
    number INTEGER NOT NULL,
    started_at INTEGER,
    started_seq INTEGER,
    ended_at INTEGER,
    ended_seq INTEGER,
    result TEXT,
    UNIQUE (job_id, number)
);
CREATE INDEX builds_started_seq ON builds (started_seq);
CREATE INDEX builds_ended_seq ON builds (ended_seq);

CREATE TABLE allocations (
    id INTEGER PRIMARY KEY,
    build_id INTEGER NOT NULL REFERENCES builds (id) ON DELETE CASCADE,
    node_id TEXT NOT NULL REFERENCES nodes (id),
    executor INTEGER NOT NULL,
    task TEXT,
    allocated_at INTEGER NOT NULL,
    allocated_seq INTEGER NOT NULL,
    released_at INTEGER,
    released_seq INTEGER,
    outcome TEXT,
    duration_ms INTEGER,
    problem TEXT
);
CREATE INDEX allocations_allocated_seq ON allocations (allocated_seq);
CREATE INDEX allocations_released_seq ON allocations (released_seq);
-- Finds the allocation a release belongs to: its executor slot, still open.
CREATE INDEX allocations_open ON allocations (build_id, node_id, executor) WHERE released_at IS NULL;
