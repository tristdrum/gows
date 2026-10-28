-- Status content remains in the existing session message store. These tables
-- hold authorization, expiry/provenance and revocation metadata only.
CREATE TABLE gows_status_capture (id INTEGER PRIMARY KEY CHECK (id = 1), data TEXT NOT NULL);
CREATE TABLE gows_status_metadata (
 id VARCHAR(100) PRIMARY KEY,
 sender VARCHAR(100) NOT NULL,
 from_me BOOLEAN NOT NULL,
 timestamp_ms BIGINT NOT NULL,
 expires_at_ms BIGINT NOT NULL,
 generation VARCHAR(100) NOT NULL
);
CREATE INDEX gows_status_history_page ON gows_status_metadata(timestamp_ms DESC,id DESC);
CREATE INDEX gows_status_history_expiry ON gows_status_metadata(expires_at_ms);
CREATE TABLE gows_status_tombstones (
 id VARCHAR(100) PRIMARY KEY,
 sender VARCHAR(100) NOT NULL,
 from_me BOOLEAN NOT NULL,
 expires_at_ms BIGINT NOT NULL
);
CREATE INDEX gows_status_tombstone_expiry ON gows_status_tombstones(expires_at_ms);
