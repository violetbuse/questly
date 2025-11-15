import gleam/bool
import gleam/dict
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import pog
import questly/kv
import questly/lock
import questly/metrics
import questly/pubsub
import questly/pubsub_store
import questly/subscriber
import questly/swim
import questly/tenant/sql

pub opaque type TenantRateLimiter {
  TenantRateLimiter(id: String, pubsub: pubsub.Pubsub, kv: kv.Kv)
}

pub fn new(id: String, pubsub: pubsub.Pubsub, kv: kv.Kv) {
  TenantRateLimiter(id, pubsub, kv)
}

pub fn consume(limiter: TenantRateLimiter) {
  let expiry =
    timestamp.system_time()
    |> timestamp.add(duration.minutes(3))
    |> timestamp.to_unix_seconds
    |> float.round

  let channel = "ch_tenant_rate_limit_" <> limiter.id
  let id = "decrement"

  pubsub.publish(limiter.pubsub, channel, id, "", dict.new(), expiry)
}

pub fn refresh(limiter: TenantRateLimiter) {
  let expiry =
    timestamp.system_time()
    |> timestamp.add(duration.minutes(3))
    |> timestamp.to_unix_seconds
    |> float.round

  let channel = "ch_tenant_rate_limit_" <> limiter.id
  let id = "refresh"

  pubsub.publish(limiter.pubsub, channel, id, "", dict.new(), expiry)
}

pub fn get_tokens(limiter: TenantRateLimiter) {
  let key = "tenant_rate_limit_" <> limiter.id
  kv.get(limiter.kv, key)
  |> result.try(fn(value) {
    case value {
      value if value.deleted == True -> Error(Nil)
      value -> Ok(value.data)
    }
  })
  |> result.map(int.parse)
  |> result.flatten
  |> result.unwrap(5)
}

pub type TenantRateLimiterConfig {
  TenantRateLimiterConfig(
    id: String,
    db: process.Name(pog.Message),
    pubsub: pubsub.Pubsub,
    kv: kv.Kv,
    swim: swim.Swim,
  )
}

pub opaque type Message {
  Heartbeat
  Refresh
  Commit
  Increment
  Decrement
}

type State {
  State(
    id: String,
    subject: process.Subject(Message),
    db: process.Name(pog.Message),
    lock: lock.Lock,
    subscriber: subscriber.Subscriber(Message),
    kv: kv.Kv,
    kv_entry: String,
    per_day_limit: Int,
    tokens: Int,
    queued_change: Int,
  )
}

fn initialize(
  self: process.Subject(Message),
  config: TenantRateLimiterConfig,
) -> Result(actor.Initialised(State, Message, process.Subject(Message)), String) {
  let self_node = swim.get_self(config.swim)

  let ratelimit_lock =
    lock.new_locked(config.db, self_node.id, "tenant_rate_limit_" <> config.id)

  let channel = "ch_tenant_rate_limit_" <> config.id
  let ratelimit_subscriber =
    subscriber.new(
      config.pubsub,
      self,
      subscriber_mapper(_, channel),
      option.None,
    )

  let initial_state =
    State(
      subject: self,
      id: config.id,
      db: config.db,
      lock: ratelimit_lock,
      subscriber: ratelimit_subscriber,
      kv: config.kv,
      kv_entry: "tenant_rate_limit_" <> config.id,
      per_day_limit: 1000,
      tokens: 100,
      queued_change: 0,
    )

  process.send_after(self, int.random(commit_period), Commit)
  process.send_after(self, int.random(5000), Heartbeat)
  process.send_after(self, 5000, Increment)

  actor.initialised(initial_state)
  |> actor.returning(self)
  |> Ok
}

fn subscriber_mapper(event: pubsub_store.Event, channel: String) {
  use <- bool.guard(when: event.channel != channel, return: Error(Nil))

  case event.id {
    "decrement" -> Ok(Decrement)
    "refresh" -> Ok(Refresh)
    _ -> Error(Nil)
  }
}

const minimum_period = 20_000.0

/// returns #(period, amount)
fn compute_increment_period_and_amount(per_day_limit: Int) -> #(Int, Int) {
  let day_ms = { 24 * 60 * 60 * 1000 } |> int.to_float
  let base_period = day_ms /. int.to_float(per_day_limit)

  let base_factor = minimum_period /. base_period

  let factor = float.ceiling(base_factor)
  let period = base_period *. factor

  #(float.round(period), float.round(factor))
}

fn use_locked(
  state: State,
  cb: fn() -> actor.Next(State, Message),
) -> actor.Next(State, Message) {
  use <- bool.lazy_guard(
    when: lock.is_locked(state.lock) == lock.Locked,
    return: cb,
  )

  actor.continue(state)
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Heartbeat -> handle_heartbeat(state)
    Refresh -> handle_refresh(state)
    Commit -> handle_commit(state)
    Decrement -> handle_decrement(state)
    Increment -> handle_increment(state)
  }
}

const heartbeat_period = 300_000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, heartbeat_period, Heartbeat)

  let db = pog.named_connection(state.db)
  let assert Ok(rows) = sql.get_tenant(db, state.id)
  let assert [row] = rows.rows

  let tokens = {
    use <- bool.guard(when: row.tokens <= row.per_day_limit, return: row.tokens)

    let assert Ok(_) = sql.set_tokens(db, state.id, row.per_day_limit)
    row.per_day_limit
  }

  let new_state =
    State(
      ..state,
      per_day_limit: row.per_day_limit,
      tokens: tokens + state.queued_change,
    )

  actor.continue(new_state)
}

fn handle_refresh(state: State) -> actor.Next(State, Message) {
  let db = pog.named_connection(state.db)
  let assert Ok(rows) = sql.get_tenant(db, state.id)
  let assert [row] = rows.rows

  let new_state =
    State(
      ..state,
      per_day_limit: row.per_day_limit,
      tokens: row.tokens + state.queued_change,
    )

  actor.continue(new_state)
}

const commit_period = 60_000

fn handle_commit(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, commit_period, Commit)

  use <- use_locked(state)

  let db = pog.named_connection(state.db)

  let assert Ok(_) = {
    case state.queued_change {
      val if val < 0 -> {
        sql.decrement(db, state.id, val |> int.negate)
        |> result.replace(Nil)
      }
      val if val > 0 -> {
        sql.increment(db, state.id, val)
        |> result.replace(Nil)
      }
      _ -> Ok(Nil)
    }
  }

  actor.continue(State(..state, queued_change: 0))
}

fn handle_decrement(state: State) -> actor.Next(State, Message) {
  use <- use_locked(state)

  let new_state =
    State(
      ..state,
      queued_change: state.queued_change - 1,
      tokens: state.tokens - 1,
    )

  kv.set(state.kv, state.kv_entry, int.to_string(new_state.tokens))

  actor.continue(new_state)
}

fn handle_increment(state: State) -> actor.Next(State, Message) {
  let should_increment = state.tokens < state.per_day_limit

  kv.set(state.kv, state.kv_entry, int.to_string(state.tokens))

  let #(period, amount) =
    compute_increment_period_and_amount(state.per_day_limit)

  process.send_after(state.subject, period, Increment)

  use <- use_locked(state)
  use <- bool.guard(when: !should_increment, return: actor.continue(state))

  let new_state =
    State(
      ..state,
      tokens: state.tokens + amount,
      queued_change: state.queued_change + amount,
    )

  io.println(
    "Incrementing tokens for tenant "
    <> state.id
    <> " by "
    <> int.to_string(amount)
    <> " every "
    <> int.to_string(period)
    <> "ms",
  )

  let _ = metrics.increment_tenant_token_increment(amount)

  kv.set(state.kv, state.kv_entry, int.to_string(new_state.tokens))

  actor.continue(new_state)
}

pub fn start(config: TenantRateLimiterConfig) {
  actor.new_with_initialiser(500, initialize(_, config))
  |> actor.on_message(on_message)
  |> actor.start
}
