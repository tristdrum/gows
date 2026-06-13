-- Create the gows_labels table
CREATE TABLE gows_labels
(
    -- Unique identifier for the label
    id VARCHAR(100) NOT NULL,
    -- Label data (JSON)
    data TEXT NOT NULL,
    -- Primary key
    PRIMARY KEY (id)
);

-- Index for id (useful if filtering by id)
CREATE UNIQUE INDEX gows_labels_id_idx ON gows_labels (id);
