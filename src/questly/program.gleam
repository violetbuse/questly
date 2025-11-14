import dot_env/env
import gleam/bool
import gleam/erlang/process
import gleam/io
import gleam/list
import gleam/otp/static_supervisor as supervisor
import gleam/result
import gleam/string
import pog
import questly/api
import questly/kv
import questly/lock_manager
import questly/metrics
import questly/pubsub
import questly/statistics
import questly/swim
import questly/tenant_rate_limit_manager

pub type Config {
  Config(
    node_id: String,
    hostname: String,
    region: String,
    cluster_secret: String,
    api_secret: String,
    api_port: Int,
    db_name: process.Name(pog.Message),
    db_url: String,
    db_pool_size: Int,
    swim_name: process.Name(swim.Message),
    swim_port: Int,
    bootstrap_nodes: List(String),
    pubsub_name: process.Name(pubsub.Message),
    pubsub_port: Int,
    kv_name: process.Name(kv.Message),
    kv_port: Int,
    metrics_port: Int,
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

  let db_name = process.new_name("database_pool")
  let assert Ok(db_url) = env.get_string("POSTGRES_URL")
    as "$POSTGRES_URL not set"
  let db_pool_size = env.get_int_or("DB_POOL_SIZE", 10)

  let swim_name = process.new_name("swim")
  let swim_port = env.get_int_or("SWIM_PORT", 8787)

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
  let pubsub_port = env.get_int_or("PUBSUB_PORT", 1337)

  let kv_name = process.new_name("kv")
  let kv_port = env.get_int_or("KV_PORT", 1773)

  let metrics_port = env.get_int_or("METRICS_PORT", 9090)

  Config(
    node_id:,
    hostname:,
    region:,
    cluster_secret:,
    api_secret:,
    api_port:,
    db_name:,
    db_url:,
    db_pool_size:,
    swim_name:,
    swim_port:,
    bootstrap_nodes:,
    pubsub_name:,
    pubsub_port:,
    kv_name:,
    kv_port:,
    metrics_port:,
  )
}

fn db_config(config: Config) -> pog.Config {
  let assert Ok(config) =
    pog.url_config(config.db_name, config.db_url)
    |> result.map(pog.pool_size(_, config.db_pool_size))
    as "Failed to configure pog based on connection string."

  config
}

fn swim_config(config: Config) -> swim.SwimConfig {
  swim.SwimConfig(
    name: config.swim_name,
    node_id: config.node_id,
    hostname: config.hostname,
    region: config.region,
    bootstrap_hosts: config.bootstrap_nodes,
    port: config.swim_port,
    secret: config.cluster_secret,
    kv_port: config.kv_port,
    pubsub_port: config.pubsub_port,
  )
}

fn pubsub_config(config: Config) -> pubsub.PubsubConfig {
  pubsub.PubsubConfig(
    name: config.pubsub_name,
    swim: swim.from_name(config.swim_name),
    port: config.pubsub_port,
    secret: config.cluster_secret,
  )
}

fn kv_config(config: Config) -> kv.KvConfig {
  kv.KvConfig(
    name: config.kv_name,
    swim: swim.from_name(config.swim_name),
    pubsub: pubsub.from_name(config.pubsub_name),
    port: config.kv_port,
    secret: config.cluster_secret,
  )
}

fn api_config(config: Config) -> api.ApiConfig {
  api.ApiConfig(
    port: config.api_port,
    swim: swim.from_name(config.swim_name),
    pubsub: pubsub.from_name(config.pubsub_name),
    kv: kv.from_name(config.kv_name),
    db: pog.named_connection(config.db_name),
    api_secret: config.api_secret,
    cluster_secret: config.cluster_secret,
  )
}

fn lock_manager_config(config: Config) -> lock_manager.LockManagerConfig {
  lock_manager.LockManagerConfig(deletion_period: 5000, db_name: config.db_name)
}

fn tenant_rate_limit_manager_config(
  config: Config,
) -> tenant_rate_limit_manager.TenantRateLimitManagerConfig {
  tenant_rate_limit_manager.TenantRateLimitManagerConfig(
    db_name: config.db_name,
    pubsub: pubsub.from_name(config.pubsub_name),
    swim: swim.from_name(config.swim_name),
    kv: kv.from_name(config.kv_name),
  )
}

fn statistics_config(config: Config) -> statistics.StatisticsConfig {
  statistics.StatisticsConfig(
    db: config.db_name,
    kv: kv.from_name(config.kv_name),
    swim: swim.from_name(config.swim_name),
  )
}

fn metrics_config(config: Config) -> metrics.MetricsConfig {
  metrics.MetricsConfig(
    port: config.metrics_port,
    cluster_secret: config.cluster_secret,
  )
}

pub fn start(config: Config) {
  let _ = metrics.initialize()

  supervisor.new(supervisor.OneForOne)
  |> supervisor.add(db_config(config) |> pog.supervised)
  |> supervisor.add(swim_config(config) |> swim.supervised)
  |> supervisor.add(pubsub_config(config) |> pubsub.supervised)
  |> supervisor.add(kv_config(config) |> kv.supervised)
  |> supervisor.add(api_config(config) |> api.supervised)
  |> supervisor.add(lock_manager_config(config) |> lock_manager.supervised)
  |> supervisor.add(
    tenant_rate_limit_manager_config(config)
    |> tenant_rate_limit_manager.supervised,
  )
  |> supervisor.add(statistics_config(config) |> statistics.supervised)
  |> supervisor.add(metrics_config(config) |> metrics.supervised)
  |> supervisor.start()
}
