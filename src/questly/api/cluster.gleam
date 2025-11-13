import gleam/json
import gleam/list
import questly/api/api_utils
import questly/api/context
import questly/swim
import questly/swim_store
import questly/tenant_rate_limit_manager
import wisp

pub fn router(req: wisp.Request, context: context.ApiContext) -> wisp.Response {
  use <- api_utils.use_auth(req, context)

  case wisp.path_segments(req) {
    [_] -> get_state(context)
    _ -> wisp.not_found()
  }
}

fn get_node_stats(
  node: swim_store.NodeInfo,
  context: context.ApiContext,
) -> json.Json {
  let tenant_rate_limit =
    tenant_rate_limit_manager.get_tenant_rate_limiter_count(context.kv, node)
  json.object([#("tenant_rate_limit_instances", json.int(tenant_rate_limit))])
}

fn get_state(context: context.ApiContext) -> wisp.Response {
  let self = swim.get_self(context.swim)
  let self_json =
    swim_store.encode_node_info_with_stats(self, get_node_stats(self, context))
  let remote =
    swim.get_remote(context.swim) |> list.sort(swim_store.compare_node)
  let remote_json =
    list.map(remote, fn(node) {
      swim_store.encode_node_info_with_stats(
        node,
        get_node_stats(node, context),
      )
    })

  let json =
    json.object([
      #("self", self_json),
      #("remote", json.preprocessed_array(remote_json)),
    ])

  let body = json.to_string(json)

  wisp.json_response(body, 200)
}
