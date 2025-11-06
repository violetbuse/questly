# create a postgres container to develop against
create-db:
    docker run -d \
      --name joblot-pg \
      -e POSTGRES_USER=postgres \
      -e POSTGRES_PASSWORD=postgres \
      -e POSTGRES_DB=postgres \
      -p 5432:5432 \
      -v joblot-pg-data:/var/lib/postgresql/data \
      postgres:17

# diff schema.sql against existing migrations to create a new migration
create-migration MIGRATION_NAME:
    atlas migrate diff {{MIGRATION_NAME}} \
        --dir "file://migrations" \
        --to "file://schema.sql" \
        --dev-url "docker://postgres/17/dev?search_path=public"

# apply migrations to dev server
apply-migrations:
    atlas migrate apply \
        --dir "file://migrations" \
        --url "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable"

# run squirrel to generate database query functions
squirrel:
    DATABASE_URL="postgres://postgres:postgres@localhost:5432/postgres" gleam run -m squirrel
