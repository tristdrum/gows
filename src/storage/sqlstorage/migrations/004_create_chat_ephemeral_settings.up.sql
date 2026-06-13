CREATE TABLE gows_chat_ephemeral_setting
(
    id VARCHAR(100) NOT NULL,
    data TEXT NOT NULL,
    PRIMARY KEY (id)
);

-- Index for id (useful if filtering by jid)
CREATE INDEX gows_chat_ephemeral_setting_id_idx ON gows_chat_ephemeral_setting (id);
