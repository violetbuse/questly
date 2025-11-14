import glatistics
import gleam/dict
import mist
import themis
import themis/counter
import themis/gauge
import themis/number
import wisp
import wisp/wisp_mist

pub type MetricsConfig {
  MetricsConfig(port: Int, cluster_secret: String)
}

fn router(req: wisp.Request) -> wisp.Response {
  case wisp.path_segments(req) {
    ["metrics"] -> metrics()
    _ -> wisp.not_found()
  }
}

fn metrics() -> wisp.Response {
  case themis.print() {
    Error(_) -> wisp.internal_server_error()
    Ok(prometheus_string) -> wisp.ok() |> wisp.string_body(prometheus_string)
  }
}

pub fn supervised(config: MetricsConfig) {
  wisp_mist.handler(router, config.cluster_secret)
  |> mist.new
  |> mist.bind("0.0.0.0")
  |> mist.with_ipv6
  |> mist.port(config.port)
  |> mist.supervised
}

pub fn initialize() {
  glatistics.enable_scheduler_wall_time(True)
  themis.init()

  let assert Ok(_) =
    gauge.new(
      "postgres_latency_ms",
      "Raw latency to the postgres cluster in ms",
    )
    as "could not create postgres latency gauge"

  let assert Ok(_) =
    gauge.new(
      "tenant_rate_limiters",
      "Number of tenant rate limit processes on the node",
    )
    as "could not create tenant rate limit count gauge"

  let assert Ok(_) =
    counter.new(
      "tenant_token_increments_total",
      "The number of tenant token increment operations performed.",
    )
    as "could not create tenant token increment count counter"

  let assert Ok(_) =
    gauge.new(
      "number_of_gcs_total",
      "The number of garbage collections performed.",
    )
    as "could not create number of gcs total gauge"

  let assert Ok(_) =
    gauge.new(
      "words_reclaimed_total",
      "The total number of words reclaimed in gc.",
    )
    as "could not create words reclaimed total gauge"

  let assert Ok(_) =
    gauge.new("total_memory_used", "Total amount of memory used.")
    as "could not create total memory used gauge"

  let assert Ok(_) =
    gauge.new(
      "active_scheduler_tasks",
      "The sum of all tasks on all schedulers.",
    )
    as "could not create active scheduler tasks gauge"

  let assert Ok(_) =
    gauge.new("scheduler_wall_time", "The scheduler wall time as a percentage.")
    as "could not create scheduler wall time gauge"
}

pub fn observe_postgres_latency(latency: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(latency)
    |> gauge.observe("postgres_latency_ms", labels, _)
}

pub fn observe_tenant_rate_limit_count(count: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(count)
    |> gauge.observe("tenant_rate_limiters", labels, _)
}

pub fn increment_tenant_token_increment(by amount: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(amount)
    |> counter.increment_by("tenant_token_increments_total", labels, _)
}

pub fn observe_number_of_gcs(count: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(count)
    |> gauge.observe("number_of_gcs_total", labels, _)
}

pub fn observe_words_reclaimed(count: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(count)
    |> gauge.observe("words_reclaimed_total", labels, _)
}

pub fn observe_total_memory_used(count: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(count)
    |> gauge.observe("total_memory_used", labels, _)
}

pub fn observe_active_scheduler_tasks(count: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(count)
    |> gauge.observe("active_scheduler_tasks", labels, _)
}

pub fn observe_scheduler_wall_time(percent: Int) {
  let labels = dict.new()

  let assert Ok(_) =
    number.integer(percent)
    |> gauge.observe("scheduler_wall_time", labels, _)
}
