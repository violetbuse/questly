import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/otp/actor
import gleam/otp/supervision
import gleam/time/timestamp
import pog
import questly/lock/sql

pub type LockManagerConfig {
  LockManagerConfig(deletion_period: Int, db_name: process.Name(pog.Message))
}

type Message {
  Heartbeat
}

type State {
  State(
    subject: process.Subject(Message),
    period: Int,
    db: process.Name(pog.Message),
  )
}

fn initialize(
  self: process.Subject(Message),
  config: LockManagerConfig,
) -> Result(actor.Initialised(State, a, Nil), String) {
  let state =
    State(subject: self, period: config.deletion_period, db: config.db_name)

  process.send_after(self, int.random(state.period), Heartbeat)

  actor.initialised(state)
  |> Ok
}

fn on_message(state: State, _message: Message) {
  process.send_after(state.subject, state.period, Heartbeat)

  let older_than = 60
  let expired_before =
    timestamp.system_time()
    |> timestamp.to_unix_seconds
    |> float.round
    |> int.subtract(older_than)

  let assert Ok(_) =
    pog.named_connection(state.db)
    |> sql.cleanup(expired_before)
    as "failed to cleanup expired locks."

  actor.continue(state)
}

pub fn supervised(config: LockManagerConfig) {
  supervision.worker(fn() {
    actor.new_with_initialiser(500, initialize(_, config))
    |> actor.on_message(on_message)
    |> actor.start
  })
}
