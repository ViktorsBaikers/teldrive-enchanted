-- +goose Up
-- +goose StatementBegin
CREATE INDEX IF NOT EXISTS idx_uploads_upload_id ON teldrive.uploads (upload_id);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP INDEX IF EXISTS teldrive.idx_uploads_upload_id;
-- +goose StatementEnd
