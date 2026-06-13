CREATE TABLE gows_groups
(
    id VARCHAR(100) NOT NULL,
    name VARCHAR(100) NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (id)
);

-- Index for jid (useful if filtering by jid)
CREATE INDEX gows_groups_id_idx ON gows_groups (id);

-- Index for name (useful if filtering by name)
CREATE INDEX gows_groups_name_idx ON gows_groups (name);
