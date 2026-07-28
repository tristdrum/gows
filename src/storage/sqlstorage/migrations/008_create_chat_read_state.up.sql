CREATE TABLE gows_chat_read_state
(
    jid VARCHAR(100) NOT NULL,
    baseline_unread_count BIGINT NOT NULL,
    marked_as_unread BOOLEAN NOT NULL,
    unread_state_known BOOLEAN NOT NULL,
    count_from_timestamp BIGINT NOT NULL,
    evidence_timestamp BIGINT NOT NULL,
    PRIMARY KEY (jid)
);

CREATE INDEX gows_chat_read_state_evidence_idx
ON gows_chat_read_state (evidence_timestamp);
