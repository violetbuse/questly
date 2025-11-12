//// This module contains the code to run the sql queries defined in
//// `./src/questly/lock/sql`.
//// > 🐿️ This module was generated automatically using v4.5.0 of
//// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
////

import gleam/dynamic/decode
import pog

/// Runs the `cleanup` query
/// defined in `./src/questly/lock/sql/cleanup.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn cleanup(
  db: pog.Connection,
  arg_1: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "DELETE FROM locks WHERE expires_at < $1;
"
  |> pog.query
  |> pog.parameter(pog.int(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `lock` query
/// defined in `./src/questly/lock/sql/lock.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn lock(
  db: pog.Connection,
  arg_1: String,
  arg_2: String,
  arg_3: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "INSERT INTO locks (resource_id, nonce, expires_at) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.int(arg_3))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// A row you get from running the `query` query
/// defined in `./src/questly/lock/sql/query.sql`.
///
/// > 🐿️ This type definition was generated automatically using v4.5.0 of the
/// > [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub type QueryRow {
  QueryRow(resource_id: String, nonce: String, expires_at: Int)
}

/// Runs the `query` query
/// defined in `./src/questly/lock/sql/query.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn query(
  db: pog.Connection,
  arg_1: String,
) -> Result(pog.Returned(QueryRow), pog.QueryError) {
  let decoder = {
    use resource_id <- decode.field(0, decode.string)
    use nonce <- decode.field(1, decode.string)
    use expires_at <- decode.field(2, decode.int)
    decode.success(QueryRow(resource_id:, nonce:, expires_at:))
  }

  "SELECT * FROM locks WHERE resource_id = $1;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `release` query
/// defined in `./src/questly/lock/sql/release.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn release(
  db: pog.Connection,
  arg_1: String,
  arg_2: String,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "DELETE FROM locks WHERE resource_id = $1 AND nonce = $2;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.returning(decoder)
  |> pog.execute(db)
}

/// Runs the `renew` query
/// defined in `./src/questly/lock/sql/renew.sql`.
///
/// > 🐿️ This function was generated automatically using v4.5.0 of
/// > the [squirrel package](https://github.com/giacomocavalieri/squirrel).
///
pub fn renew(
  db: pog.Connection,
  arg_1: String,
  arg_2: String,
  arg_3: Int,
) -> Result(pog.Returned(Nil), pog.QueryError) {
  let decoder = decode.map(decode.dynamic, fn(_) { Nil })

  "UPDATE locks
SET expires_at = $3
WHERE resource_id = $1 AND nonce = $2;
"
  |> pog.query
  |> pog.parameter(pog.text(arg_1))
  |> pog.parameter(pog.text(arg_2))
  |> pog.parameter(pog.int(arg_3))
  |> pog.returning(decoder)
  |> pog.execute(db)
}
