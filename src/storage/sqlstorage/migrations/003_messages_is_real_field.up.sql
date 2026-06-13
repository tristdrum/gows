-- gows_messages - is_real field
-- True by default and for existing messages
ALTER TABLE gows_messages ADD COLUMN is_real BOOLEAN NOT NULL DEFAULT TRUE;
