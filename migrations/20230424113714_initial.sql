CREATE TABLE IF NOT EXISTS blobs
(
    id          BIGSERIAL PRIMARY KEY,
    ulid        VARCHAR(26) NOT NULL,
    data        JSONB NOT NULL
);

CREATE INDEX blobs_ulid_idx ON blobs USING BTREE (ulid);
