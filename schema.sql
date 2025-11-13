CREATE TABLE locks (
  resource_id VARCHAR(255) NOT NULL PRIMARY KEY,
  nonce VARCHAR(255) NOT NULL,
  expires_at BIGINT NOT NULL
);

CREATE TABLE tenants (
  id VARCHAR(255) NOT NULL PRIMARY KEY,
  created_at BIGINT NOT NULL DEFAULT (EXTRACT(EPOCH FROM now())::BIGINT),
  per_day_limit INT NOT NULL,
  tokens INT NOT NULL DEFAULT 100
);

-- CREATE TABLE requests (
--   id VARCHAR(255) NOT NULL PRIMARY KEY,
--   url TEXT NOT NULL,
--   method VARCHAR(50) NOT NULL,
--   headers TEXT[] NOT NULL,
--   body TEXT NOT NULL
-- );

-- CREATE TABLE responses(
--   id VARCHAR(255) NOT NULL PRIMARY KEY,
--   status_code INTEGER NOT NULL,
--   headers TEXT[] NOT NULL,
--   body TEXT NOT NULL,
--   duration INTEGER NOT NULL,
-- );
