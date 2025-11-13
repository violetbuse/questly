-- Modify "tenants" table
ALTER TABLE "tenants" ALTER COLUMN "per_day_limit" SET NOT NULL, ALTER COLUMN "tokens" SET NOT NULL;
