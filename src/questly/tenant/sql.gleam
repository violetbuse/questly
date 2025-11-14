//// This module contains the code to run the sql queries defined in
//// `./src/questly/tenant/sql`.
//// > 🐿️ This module was generated automatically using v4.5.0 of
//// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
////

import gleam/dynamic/decode
import pog

/// Runs the `create_tenant` query
/// defined in `./src/questly/tenant/sql/create_tenant.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn create_tenant(
  db: pog.Connection,
  arg_1: String,
  arg_2: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "INSERT INTO tenants (id, per_day_limit) VALUES ($1, $2);
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `decrement` query
/// defined in `./src/questly/tenant/sql/decrement.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn decrement(
  db: pog.Connection,
  arg_1: String,
  arg_2: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "UPDATE tenants
SET tokens = tokens - $2
WHERE
id = $1 AND
tokens > 0;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_tenant` query
/// defined in `./src/questly/tenant/sql/get_tenant.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.5.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetTenantRow {
  GetTenantRow(id: String, created_at: Int, per_day_limit: Int, tokens: Int)
}

/// Runs the `get_tenant` query
/// defined in `./src/questly/tenant/sql/get_tenant.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_tenant(
  db: pog.Connection,
  arg_1: String,
) -> Result(pog.Returned(GetTenantRow), pog.QueryError) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use created_at <- decode.field(1, decode.int)
    use per_day_limit <- decode.field(2, decode.int)
    use tokens <- decode.field(3, decode.int)
    decode.success(GetTenantRow(id:, created_at:, per_day_limit:, tokens:))
  }

  "SELECT * FROM tenants WHERE id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `get_tokens` query
/// defined in `./src/questly/tenant/sql/get_tokens.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.5.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type GetTokensRow {
  GetTokensRow(tokens: Int)
}

/// Runs the `get_tokens` query
/// defined in `./src/questly/tenant/sql/get_tokens.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn get_tokens(
  db: pog.Connection,
  arg_1: String,
) -> Result(pog.Returned(GetTokensRow), pog.QueryError) {
  let decoder = {
    use tokens <- decode.field(0, decode.int)
    decode.success(GetTokensRow(tokens:))
  }

  "SELECT tokens FROM tenants WHERE id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `increment` query
/// defined in `./src/questly/tenant/sql/increment.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn increment(
  db: pog.Connection,
  arg_1: String,
  arg_2: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "UPDATE tenants
SET tokens = tokens + $2
WHERE
  id = $1 AND
  tokens < per_day_limit;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `list_tenants` query
/// defined in `./src/questly/tenant/sql/list_tenants.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.5.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type ListTenantsRow {
  ListTenantsRow(id: String, created_at: Int, per_day_limit: Int, tokens: Int)
}

/// Runs the `list_tenants` query
/// defined in `./src/questly/tenant/sql/list_tenants.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn list_tenants(
  db: pog.Connection,
  arg_1: String,
) -> Result(pog.Returned(ListTenantsRow), pog.QueryError) {
  let decoder = {
    use id <- decode.field(0, decode.string)
    use created_at <- decode.field(1, decode.int)
    use per_day_limit <- decode.field(2, decode.int)
    use tokens <- decode.field(3, decode.int)
    decode.success(ListTenantsRow(id:, created_at:, per_day_limit:, tokens:))
  }

  "SELECT * FROM tenants WHERE id > $1 ORDER BY id ASC LIMIT 1000;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `set_limit` query
/// defined in `./src/questly/tenant/sql/set_limit.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn set_limit(
  db: pog.Connection,
  arg_1: String,
  arg_2: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "UPDATE tenants
SET per_day_limit = $2
WHERE id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `set_tokens` query
/// defined in `./src/questly/tenant/sql/set_tokens.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn set_tokens(
  db: pog.Connection,
  arg_1: String,
  arg_2: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "UPDATE tenants SET tokens = $2 WHERE id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.int(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}
