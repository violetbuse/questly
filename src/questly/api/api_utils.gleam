import gleam/bool
import gleam/list
import gleam/result
import gleam/string
import questly/api/context
import wisp

pub fn use_auth(
  req: wisp.Request,
  context: context.ApiContext,
  cb: fn() -> wisp.Response,
) -> wisp.Response {
  let unauthed_response = wisp.response(401)
  let key = context.cluster_secret

  let query = wisp.get_query(req)

  let query_secret_valid =
    list.key_find(query, "secret") |> result.unwrap("") == key
  let query_secret_key_valid =
    list.key_find(query, "secret_key") |> result.unwrap("") == key

  let auth_header =
    req.headers |> list.key_find("authorization") |> result.unwrap("")

  let authorization_valid = auth_header == key
  let bearer_valid = string.replace(auth_header, "Bearer ", "") == key

  let authorized =
    query_secret_valid
    || query_secret_key_valid
    || authorization_valid
    || bearer_valid

  use <- bool.guard(when: !authorized, return: unauthed_response)

  cb()
}
