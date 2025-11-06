import gleam/json
import gleam/list
import questly/api/api_utils
import questly/api/context
import questly/swim
import questly/swim_store
import wisp

pub fn router(req: wisp.Request, context: context.ApiContext) -> wisp.Response {
  use <- api_utils.use_auth(req, context)

  case wisp.path_segments(req) {
    [_] -> get_state(context)
    _ -> wisp.not_found()
  }
}

fn get_state(context: context.ApiContext) -> wisp.Response {
  let self = swim.get_self(context.swim)
  let remote =
    swim.get_remote(context.swim) |> list.sort(swim_store.compare_node)

  let json =
    json.object([
      #("self", swim_store.encode_node_info(self)),
      #("remote", json.array(remote, swim_store.encode_node_info)),
    ])

  let body = json.to_string(json)

  wisp.json_response(body, 200)
}
