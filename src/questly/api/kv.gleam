import gleam/http
import gleam/json
import gleam/option
import questly/api/api_utils
import questly/api/context
import questly/kv
import questly/kv_store
import wisp

pub fn router(req: wisp.Request, context: context.ApiContext) -> wisp.Response {
  use <- api_utils.use_auth(req, context)

  case req.method, wisp.path_segments(req) {
    http.Get, [_, key] -> handle_get_value(key, context)
    http.Post, [_, key] -> handle_set_value(req, key, context)
    http.Delete, [_, key] -> handle_delete_value(key, context)
    _, _ -> wisp.not_found()
  }
}

fn handle_get_value(key: String, context: context.ApiContext) -> wisp.Response {
  kv.get(context.kv, key)
  |> option.from_result
  |> json.nullable(kv_store.encode_value)
  |> json.to_string
  |> wisp.json_response(200)
}

fn handle_set_value(
  req: wisp.Request,
  key: String,
  context: context.ApiContext,
) -> wisp.Response {
  use value <- wisp.require_string_body(req)

  kv.set(context.kv, key, value)

  json.object([#("success", json.bool(True))])
  |> json.to_string
  |> wisp.json_response(200)
}

fn handle_delete_value(
  key: String,
  context: context.ApiContext,
) -> wisp.Response {
  kv.delete(context.kv, key)

  json.object([#("success", json.bool(True))])
  |> json.to_string
  |> wisp.json_response(200)
}
