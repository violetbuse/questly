import mist
import pog
import questly/api/cluster
import questly/api/context
import questly/api/kv as kv_api
import questly/api/pubsub as pubsub_api
import questly/api/tenant as tenant_api
import questly/kv
import questly/pubsub
import questly/swim
import wisp
import wisp/wisp_mist

pub type ApiConfig {
  ApiConfig(
    port: Int,
    swim: swim.Swim,
    pubsub: pubsub.Pubsub,
    kv: kv.Kv,
    api_secret: String,
    db: pog.Connection,
    cluster_secret: String,
  )
}

fn router(req: wisp.Request, context: context.ApiContext) -> wisp.Response {
  case wisp.path_segments(req) {
    ["health"] -> health(req)
    ["cluster", ..] -> cluster.router(req, context)
    ["pubsub", ..] -> pubsub_api.router(req, context)
    ["kv", ..] -> kv_api.router(req, context)
    ["tenants", ..] -> tenant_api.router(req, context)
    _ -> wisp.not_found()
  }
}

fn health(_req: wisp.Request) -> wisp.Response {
  wisp.ok()
}

pub fn supervised(config: ApiConfig) {
  let context =
    context.Context(
      swim: config.swim,
      pubsub: config.pubsub,
      kv: config.kv,
      db: config.db,
      cluster_secret: config.cluster_secret,
    )

  wisp_mist.handler(router(_, context), config.api_secret)
  |> mist.new
  |> mist.bind("0.0.0.0")
  |> mist.with_ipv6
  |> mist.port(config.port)
  |> mist.supervised
}
