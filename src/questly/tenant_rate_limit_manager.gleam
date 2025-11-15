import gleam/bool
import gleam/dict
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/io
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/factory_supervisor
import gleam/otp/supervision
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import pog
import questly/hash
import questly/kv
import questly/metrics
import questly/pubsub
import questly/pubsub_store
import questly/subscriber
import questly/swim
import questly/swim_store
import questly/tenant/sql
import questly/tenant_rate_limiter

pub type TenantRateLimitManagerConfig {
  TenantRateLimitManagerConfig(
    db_name: process.Name(pog.Message),
    pubsub: pubsub.Pubsub,
    swim: swim.Swim,
    kv: kv.Kv,
  )
}

pub fn notify_new_tenant(pubsub: pubsub.Pubsub, tenant_id: String) {
  let expiry =
    timestamp.system_time()
    |> timestamp.add(duration.minutes(3))
    |> timestamp.to_unix_seconds
    |> float.round

  pubsub.publish(
    pubsub,
    pubsub_channel,
    new_tenant_id,
    tenant_id,
    dict.new(),
    expiry,
  )
}

const pubsub_channel = "tenant_rate_limit_manager"

const new_tenant_id = "new_tenant_id"

type Message {
  Heartbeat
  NewTenant(id: String)
  CreateTenant(id: String)
}

type State {
  State(
    subject: process.Subject(Message),
    db: process.Name(pog.Message),
    subscriber: subscriber.Subscriber(Message),
    swim: swim.Swim,
    kv: kv.Kv,
    factory: factory_supervisor.Supervisor(
      String,
      process.Subject(tenant_rate_limiter.Message),
    ),
    rate_limiters: dict.Dict(
      String,
      process.Subject(tenant_rate_limiter.Message),
    ),
  )
}

fn initialize(
  self: process.Subject(Message),
  config: TenantRateLimitManagerConfig,
) -> Result(actor.Initialised(State, Message, Nil), String) {
  let subscriber =
    subscriber.new(config.pubsub, self, subscriber_mapper, option.None)

  let assert Ok(factory_supervisor) =
    factory_supervisor.worker_child(factory_worker_child(_, config))
    |> factory_supervisor.start

  let state =
    State(
      subject: self,
      db: config.db_name,
      subscriber:,
      factory: factory_supervisor.data,
      swim: config.swim,
      kv: config.kv,
      rate_limiters: dict.new(),
    )

  process.send_after(self, int.random(1000), Heartbeat)

  let self = swim.get_self(state.swim)
  let _ = set_tenant_rate_limiter_count(state.kv, self, 0)

  actor.initialised(state)
  |> Ok
}

fn subscriber_mapper(event: pubsub_store.Event) -> Result(Message, Nil) {
  use <- bool.guard(when: event.channel != pubsub_channel, return: Error(Nil))

  case event.id {
    event_id if event_id == new_tenant_id -> Ok(NewTenant(event.data))
    _ -> Error(Nil)
  }
}

fn factory_worker_child(
  tenant_id: String,
  config: TenantRateLimitManagerConfig,
) -> Result(
  actor.Started(process.Subject(tenant_rate_limiter.Message)),
  actor.StartError,
) {
  tenant_rate_limiter.start(tenant_rate_limiter.TenantRateLimiterConfig(
    id: tenant_id,
    db: config.db_name,
    pubsub: config.pubsub,
    kv: config.kv,
    swim: config.swim,
  ))
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    CreateTenant(id:) -> handle_create_tenant(state, id)
    Heartbeat -> handle_heartbeat(state)
    NewTenant(id:) -> handle_new_tenant(state, id)
  }
}

const heartbeat_interval = 300_000

fn heartbeat_get_loop(
  db: pog.Connection,
  accumulator: List(sql.ListTenantsRow),
  cursor: String,
) {
  let assert Ok(pog.Returned(count: _, rows:)) = sql.list_tenants(db, cursor)

  case rows {
    [] -> accumulator
    rows -> {
      let assert Ok(last) = list.last(rows)
      let new_accumulator = list.append(accumulator, rows)
      heartbeat_get_loop(db, new_accumulator, last.id)
    }
  }
}

fn is_replica(id: String, state: State) -> Bool {
  let self = swim.get_self(state.swim)
  let remote =
    swim.get_remote(state.swim)
    |> list.filter(swim_store.is_primary_region)
    |> list.filter(swim_store.is_alive)

  hash.is_current_node_replica(id, 2, self, remote)
}

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.spawn(fn() {
    let db = pog.named_connection(state.db)
    let rows = heartbeat_get_loop(db, [], "")

    list.each(rows, fn(row) {
      use <- bool.guard(
        when: dict.has_key(state.rate_limiters, row.id),
        return: Ok(Nil),
      )
      process.send(state.subject, NewTenant(row.id))
      Ok(Nil)
    })
  })

  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  let process_count = dict.size(state.rate_limiters)
  let self = swim.get_self(state.swim)
  let _ = set_tenant_rate_limiter_count(state.kv, self, process_count)

  actor.continue(state)
}

fn handle_new_tenant(state: State, id: String) -> actor.Next(State, Message) {
  process.spawn(fn() {
    let existing = dict.has_key(state.rate_limiters, id)

    use <- bool.guard(when: existing, return: Nil)
    use <- bool.guard(when: !is_replica(id, state), return: Nil)

    let db = pog.named_connection(state.db)
    case sql.get_tenant(db, id) {
      Ok(pog.Returned(count: 1, rows: [tenant])) ->
        process.send(state.subject, CreateTenant(tenant.id))
      _ -> Nil
    }
  })

  actor.continue(state)
}

const instances_meter_prefix = "tenant_rate_limiter_instances_"

fn handle_create_tenant(state: State, id: String) -> actor.Next(State, Message) {
  use <- bool.guard(
    when: dict.has_key(state.rate_limiters, id),
    return: actor.continue(state),
  )

  io.println("Starting tenant_rate_limiter instance for " <> id)

  let assert Ok(tenant_rate_limit) =
    factory_supervisor.start_child(state.factory, id)

  let rate_limiters =
    dict.insert(state.rate_limiters, id, tenant_rate_limit.data)

  let rate_limiter_count = dict.size(rate_limiters)
  let self = swim.get_self(state.swim)
  let _ = set_tenant_rate_limiter_count(state.kv, self, rate_limiter_count)

  actor.continue(State(..state, rate_limiters:))
}

fn set_tenant_rate_limiter_count(
  kv: kv.Kv,
  node: swim_store.NodeInfo,
  count: Int,
) {
  kv.set(kv, instances_meter_prefix <> node.id, int.to_string(count))
  metrics.observe_tenant_rate_limit_count(count)
}

pub fn get_tenant_rate_limiter_count(kv: kv.Kv, node: swim_store.NodeInfo) {
  kv.get(kv, instances_meter_prefix <> node.id)
  |> result.map(fn(value) { int.parse(value.data) })
  |> result.flatten
  |> result.unwrap(0)
}

pub fn supervised(
  config: TenantRateLimitManagerConfig,
) -> supervision.ChildSpecification(Nil) {
  supervision.worker(fn() {
    actor.new_with_initialiser(1000, initialize(_, config))
    |> actor.on_message(on_message)
    |> actor.start
  })
}
