import dot_env/env
import gleam/bool
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/otp/static_supervisor as supervisor
import gleam/string
import questly/api
import questly/pubsub
import questly/swim

pub type Config {
  Config(
    node_id: String,
    hostname: String,
    region: String,
    cluster_secret: String,
    api_secret: String,
    api_port: Int,
    swim_name: process.Name(swim.Message),
    bootstrap_nodes: List(String),
    pubsub_name: process.Name(pubsub.Message),
  )
}

pub fn generate_config() -> Config {
  let assert Ok(node_id) = env.get_string("SERVER_ID") as "$SERVER_ID not set"
  let assert Ok(hostname) = env.get_string("HOSTNAME") as "$HOSTNAME not set"
  let assert Ok(region) = env.get_string("REGION") as "$REGION not set"
  let assert Ok(cluster_secret) = env.get_string("SECRET") as "$SECRET not set"
  let assert Ok(api_secret) = env.get_string("API_SECRET")
    as "$API_SECRET not set"
  let assert Ok(api_port) = env.get_int("PORT") as "$PORT not set"

  let swim_name = process.new_name("swim")

  let bootstrap_nodes =
    env.get_string_or("BOOTSTRAP_NODES", "")
    |> string.split(",")
    |> list.map(string.trim)
    |> list.filter(fn(str) { string.is_empty(str) |> bool.negate })

  io.println(
    "starting with bootstrap nodes: \n"
    <> list.map(bootstrap_nodes, string.append("    ", _)) |> string.join("\n"),
  )

  let pubsub_name = process.new_name("pubsub")

  Config(
    node_id:,
    hostname:,
    region:,
    cluster_secret:,
    api_secret:,
    api_port:,
    swim_name:,
    bootstrap_nodes:,
    pubsub_name:,
  )
}

fn swim_config(config: Config) -> swim.SwimConfig {
  swim.SwimConfig(
    name: config.swim_name,
    node_id: config.node_id,
    hostname: config.hostname,
    region: config.region,
    bootstrap_hosts: config.bootstrap_nodes,
    port: 8787,
    secret: config.cluster_secret,
  )
}

fn pubsub_config(config: Config) -> pubsub.PubsubConfig {
  pubsub.PubsubConfig(
    name: config.pubsub_name,
    swim: swim.from_name(config.swim_name),
    port: 1337,
    secret: config.cluster_secret,
  )
}

fn api_config(config: Config) -> api.ApiConfig {
  api.ApiConfig(
    port: config.api_port,
    swim: swim.from_name(config.swim_name),
    pubsub: pubsub.from_name(config.pubsub_name),
    api_secret: config.api_secret,
    cluster_secret: config.cluster_secret,
  )
}

pub fn start(config: Config) {
  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(swim_config(config) |> swim.supervised)
  |> supervisor.add(pubsub_config(config) |> pubsub.supervised)
  |> supervisor.add(api_config(config) |> api.supervised)
  |> supervisor.start()
}
