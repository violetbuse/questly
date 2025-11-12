CREATE TABLE locks (
  resource_id VARCHAR(255) PRIMARY KEY,
  nonce VARCHAR(255),
  expires_at BIGINT
);
