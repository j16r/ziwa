CREATE TYPE job_type AS ENUM ('shutdown', 'files_add');

CREATE TABLE IF NOT EXISTS jobs
(
    id          BIGSERIAL PRIMARY KEY,
    job         job_type NOT NULL,
    data        JSONB NOT NULL
);

CREATE TABLE IF NOT EXISTS blobs
(
    id          BIGSERIAL PRIMARY KEY,
    ulid        VARCHAR(26) NOT NULL,
    data        JSONB NOT NULL
);

CREATE INDEX blobs_ulid_idx ON blobs USING BTREE (ulid);
