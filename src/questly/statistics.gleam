import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import pog
import questly/kv
import questly/swim
import questly/swim_store

pub type StatisticsConfig {
  StatisticsConfig(db: process.Name(pog.Message), kv: kv.Kv, swim: swim.Swim)
}

pub opaque type Message {
  Heartbeat
  RegisterPostgresLatency(value: Int)
}

type State {
  State(
    subject: process.Subject(Message),
    db: process.Name(pog.Message),
    kv: kv.Kv,
    swim: swim.Swim,
    postgres_latencies: List(Int),
  )
}

fn initialize(
  self: process.Subject(Message),
  config: StatisticsConfig,
) -> Result(actor.Initialised(State, Message, Nil), String) {
  let state =
    State(
      subject: self,
      db: config.db,
      kv: config.kv,
      swim: config.swim,
      postgres_latencies: [],
    )

  process.send(self, Heartbeat)

  actor.initialised(state)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Heartbeat -> handle_heartbeat(state)
    RegisterPostgresLatency(value:) ->
      handle_register_postgres_latency(state, value)
  }
}

const heartbeat_interval = 5000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  let _ = test_postgres_latency(state)

  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  actor.continue(state)
}

fn test_postgres_latency(state: State) {
  process.spawn(fn() {
    let db = pog.named_connection(state.db)
    let sql = "SELECT 1;"

    let start = timestamp.system_time()
    let assert Ok(_) =
      pog.query(sql)
      |> pog.returning(decode.dynamic)
      |> pog.execute(db)
    let end = timestamp.system_time()

    let difference = timestamp.difference(start, end)
    let milliseconds =
      duration.to_seconds(difference) |> float.multiply(1000.0) |> float.round

    process.send(state.subject, RegisterPostgresLatency(milliseconds))
  })
}

const postgres_latency_prefix = "postgres_latency_stat_"

fn publish_postgres_latency(state: State) {
  let latency =
    list.fold(state.postgres_latencies, 0, int.add)
    |> int.divide(list.length(state.postgres_latencies))

  use latency <- result.map(latency)

  let self = swim.get_self(state.swim)
  let key = postgres_latency_prefix <> self.id
  kv.set(state.kv, key, int.to_string(latency))
}

pub fn get_postgres_latency(
  kv: kv.Kv,
  node: swim_store.NodeInfo,
) -> option.Option(Int) {
  let key = postgres_latency_prefix <> node.id
  kv.get(kv, key)
  |> result.map(fn(value) { int.parse(value.data) })
  |> result.flatten
  |> option.from_result
}

fn handle_register_postgres_latency(
  state: State,
  value: Int,
) -> actor.Next(State, Message) {
  let new_latencies = [value, ..list.take(state.postgres_latencies, 4)]
  let new_state = State(..state, postgres_latencies: new_latencies)

  let _ = publish_postgres_latency(new_state)

  actor.continue(new_state)
}

pub fn supervised(
  config: StatisticsConfig,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() {
    actor.new_with_initialiser(1000, initialize(_, config))
    |> actor.on_message(on_message)
    |> actor.start
  })
}
