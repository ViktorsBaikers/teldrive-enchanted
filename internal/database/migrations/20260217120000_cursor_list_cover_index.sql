-- +goose Up
-- Cover cursor-based listings (user_id + parent_id + status + id) with commonly returned columns.
-- This reduces heap fetches for large directory listings.
DROP INDEX IF EXISTS teldrive.idx_files_id_browsing;
CREATE INDEX IF NOT EXISTS idx_files_id_browsing
ON teldrive.files (user_id, parent_id, status, id DESC)
INCLUDE (name, size, mime_type, type, updated_at, encrypted, category, hash, channel_id);

-- +goose Down
-- Restore the previous, smaller covering index definition.
DROP INDEX IF EXISTS teldrive.idx_files_id_browsing;
CREATE INDEX IF NOT EXISTS idx_files_id_browsing
ON teldrive.files (user_id, parent_id, status, id DESC)
INCLUDE (name, size, mime_type, type);

