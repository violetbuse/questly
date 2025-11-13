-- Create "tenants" table
CREATE TABLE "tenants" (
  "id" character varying(255) NOT NULL,
  "created_at" bigint NOT NULL DEFAULT (EXTRACT(epoch FROM now()))::bigint,
  "per_day_limit" integer NULL,
  "tokens" integer NULL DEFAULT 100,
  PRIMARY KEY ("id")
);
