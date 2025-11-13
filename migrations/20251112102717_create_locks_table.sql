-- Create "locks" table
CREATE TABLE "locks" (
  "resource_id" character varying(255) NOT NULL,
  "nonce" character varying(255) NULL,
  "expires_at" bigint NULL,
  PRIMARY KEY ("resource_id")
);
