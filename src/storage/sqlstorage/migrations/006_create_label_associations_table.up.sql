-- Create the gows_label_associations table
CREATE TABLE gows_label_associations
(
    -- JID (chat or contact)
    jid VARCHAR(100) NOT NULL,
    -- Label ID (foreign key to gows_labels)
    label_id VARCHAR(100) NOT NULL,
    -- Association data (JSON)
    data TEXT NOT NULL,
    -- Primary key constraint on jid and label_id
    PRIMARY KEY (jid, label_id),
    -- Foreign key constraint
    FOREIGN KEY (label_id) REFERENCES gows_labels (id) ON DELETE CASCADE
);

-- Index for jid (useful for retrieving labels for a chat)
CREATE INDEX gows_label_associations_jid_idx ON gows_label_associations (jid);

-- Index for label_id (useful for retrieving chats with a specific label)
CREATE INDEX gows_label_associations_label_id_idx ON gows_label_associations (label_id);
