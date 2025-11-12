-- Modify "locks" table
ALTER TABLE "locks" ALTER COLUMN "nonce" SET NOT NULL, ALTER COLUMN "expires_at" SET NOT NULL;
