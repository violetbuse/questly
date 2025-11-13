import gleam/bool
import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/function
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import questly/kv_store.{type Value, Value}
import questly/network_channel
import questly/pubsub
import questly/pubsub_store
import questly/subscriber
import questly/swim
import questly/swim_store

pub opaque type Kv {
  Kv(subject: process.Subject(Message))
}

pub fn from_name(name: process.Name(Message)) {
  Kv(process.named_subject(name))
}

pub type KvConfig {
  KvConfig(
    name: process.Name(Message),
    swim: swim.Swim,
    pubsub: pubsub.Pubsub,
    port: Int,
    secret: String,
  )
}

pub opaque type Message {
  Heartbeat
  AntiEntropySync
  HealthCheck(recv: process.Subject(Nil))
  LinkApi(pid: process.Pid)
  AnnounceTimes(from: String, time_map: dict.Dict(String, Int))
  GetValues(keys: List(String), recv: process.Subject(Response))
  Updates(data: dict.Dict(String, Value))
  IncomingUpdate(key: String, value: Value)
  Get(key: String, recv: process.Subject(Result(Value, Nil)))
  Put(key: String, data: String)
  Delete(key: String)
  List(prefix: String, recv: process.Subject(List(#(String, Value))))
}

type State {
  State(
    store: kv_store.KvStore,
    subject: process.Subject(Message),
    subscriber: subscriber.Subscriber(Message),
    network_channel: network_channel.Channel(Request, Response),
    swim: swim.Swim,
    port: Int,
    secret: String,
  )
}

const kv_channel = "questly_internal_kv"

fn initialize(
  self: process.Subject(Message),
  config: KvConfig,
) -> Result(actor.Initialised(State, Message, Kv), String) {
  let result = Kv(self)

  let assert Ok(kv_store) = kv_store.new() as "kv could not start kv store"

  let assert Ok(network_channel) =
    network_channel.new()
    |> network_channel.port(config.port)
    |> network_channel.get_port(get_port)
    |> network_channel.encode_message(encode_request)
    |> network_channel.decode_message(decode_request())
    |> network_channel.message_mapper(request_map)
    |> network_channel.encode_response(encode_response)
    |> network_channel.decode_response(decode_response())
    |> network_channel.response_mapper(response_map)
    |> network_channel.sender(self)
    |> network_channel.cluster_secret(config.secret)
    |> network_channel.start

  let subscriber_mapper = fn(event: pubsub_store.Event) -> Result(Message, Nil) {
    use <- bool.guard(when: event.channel != kv_channel, return: Error(Nil))

    let decoder = {
      use from <- decode.field("from", decode.string)
      use time_map <- decode.field(
        "time_map",
        decode.dict(decode.string, decode.int),
      )

      decode.success(AnnounceTimes(from:, time_map:))
    }

    json.parse(event.data, decoder) |> result.replace_error(Nil)
  }

  let kv_subscriber =
    subscriber.new(config.pubsub, self, subscriber_mapper, option.Some(0))

  let state =
    State(
      store: kv_store,
      subject: self,
      subscriber: kv_subscriber,
      network_channel:,
      swim: config.swim,
      port: config.port,
      secret: config.secret,
    )

  process.send(self, Heartbeat)
  process.send(self, AntiEntropySync)

  actor.initialised(state)
  |> actor.returning(result)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Heartbeat -> handle_heartbeat(state)
    AntiEntropySync -> handle_anti_entropy_sync(state)
    HealthCheck(recv) -> handle_health_check(state, recv)
    LinkApi(pid:) -> handle_link_api(state, pid)
    Updates(data) -> handle_updates(state, data)
    IncomingUpdate(key:, value:) -> handle_incoming_update(state, key, value)
    AnnounceTimes(from:, time_map:) ->
      handle_announce_times(state, from, time_map)
    GetValues(keys:, recv:) -> handle_get_values(state, keys, recv)
    Delete(key:) -> handle_delete(state, key)
    Get(key:, recv:) -> handle_get(state, key, recv)
    List(prefix:, recv:) -> handle_list(state, prefix, recv)
    Put(key:, data:) -> handle_put(state, key, data)
  }
}

fn send_time_announcement(
  subscriber: subscriber.Subscriber(Message),
  from: swim_store.NodeInfo,
  time_map: dict.Dict(String, Int),
) {
  let data =
    json.object([
      #("from", json.string(from.id)),
      #("time_map", json.dict(time_map, function.identity, json.int)),
    ])
    |> json.to_string

  let one_minute =
    timestamp.system_time()
    |> timestamp.add(duration.minutes(1))
    |> timestamp.to_unix_seconds
    |> float.round

  subscriber.publish(
    subscriber,
    kv_channel,
    "time_announcement",
    data,
    dict.new(),
    one_minute,
  )
}

const heartbeat_interval = 10_000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  let self = swim.get_self(state.swim)
  let to_sync =
    kv_store.sample(state.store, 7)
    |> list.map(fn(kv) { #(kv.0, { kv.1 }.version) })
    |> dict.from_list

  send_time_announcement(state.subscriber, self, to_sync)

  actor.continue(state)
}

const anti_entropy_sync_interval = 600_000

fn handle_anti_entropy_sync(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, anti_entropy_sync_interval, AntiEntropySync)

  let self = swim.get_self(state.swim)
  let to_sync =
    kv_store.list(state.store, "")
    |> list.map(fn(kv) { #(kv.0, { kv.1 }.version) })
    |> dict.from_list

  send_time_announcement(state.subscriber, self, to_sync)

  actor.continue(state)
}

fn handle_health_check(
  state: State,
  recv: process.Subject(Nil),
) -> actor.Next(State, Message) {
  process.send(recv, Nil)

  actor.continue(state)
}

fn handle_link_api(state: State, pid: process.Pid) -> actor.Next(State, Message) {
  process.link(pid)

  actor.continue(state)
}

fn handle_announce_times(
  state: State,
  from: String,
  time_map: dict.Dict(String, Int),
) -> actor.Next(State, Message) {
  process.spawn(fn() {
    let keys =
      dict.filter(time_map, fn(key, remote_time) {
        let local_time =
          kv_store.read(state.store, key)
          |> result.map(fn(value) { value.version })
          |> result.unwrap(0)

        remote_time > local_time
      })
      |> dict.keys

    use <- bool.guard(when: list.is_empty(keys), return: Ok(Nil))

    let node =
      swim.get_remote(state.swim)
      |> list.find(fn(node) { node.id == from })

    use node <- result.try(node)

    network_channel.send_request(
      state.network_channel,
      node,
      RequestValues(keys),
    )

    Ok(Nil)
  })

  actor.continue(state)
}

fn handle_get_values(
  state: State,
  keys: List(String),
  recv: process.Subject(Response),
) -> actor.Next(State, Message) {
  kv_store.list(state.store, "")
  |> list.filter(fn(kv) { list.contains(keys, kv.0) })
  |> dict.from_list
  |> Values
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_delete(state: State, key: String) -> actor.Next(State, Message) {
  let now = timestamp.system_time() |> timestamp.to_unix_seconds |> float.round
  let new_value =
    kv_store.read(state.store, key)
    |> result.map(fn(value) {
      case value.deleted {
        True -> value
        False ->
          Value(
            ..value,
            deleted: True,
            version: int.max(value.version + 1, now),
          )
      }
    })
    |> result.unwrap(Value(data: "", deleted: True, version: now))

  kv_store.write(state.store, key, new_value)

  let self = swim.get_self(state.swim)
  send_time_announcement(
    state.subscriber,
    self,
    [#(key, new_value.version)] |> dict.from_list,
  )

  actor.continue(state)
}

fn handle_put(
  state: State,
  key: String,
  data: String,
) -> actor.Next(State, Message) {
  let now = timestamp.system_time() |> timestamp.to_unix_seconds |> float.round
  let new_value =
    kv_store.read(state.store, key)
    |> result.map(fn(value) {
      case value.deleted == False && value.data == data {
        True -> value
        False ->
          Value(
            ..value,
            deleted: False,
            version: int.max(value.version + 1, now),
          )
      }
    })
    |> result.unwrap(Value(data:, deleted: False, version: now))

  kv_store.write(state.store, key, new_value)

  let self = swim.get_self(state.swim)
  send_time_announcement(
    state.subscriber,
    self,
    [#(key, new_value.version)] |> dict.from_list,
  )

  actor.continue(state)
}

fn handle_get(
  state: State,
  key: String,
  recv: process.Subject(Result(Value, Nil)),
) -> actor.Next(State, Message) {
  let value =
    kv_store.read(state.store, key)
    |> result.try(fn(value) {
      case value.deleted {
        True -> Error(Nil)
        False -> Ok(value)
      }
    })

  process.send(recv, value)

  actor.continue(state)
}

fn handle_list(
  state: State,
  prefix: String,
  recv: process.Subject(List(#(String, Value))),
) -> actor.Next(State, Message) {
  kv_store.list(state.store, prefix)
  |> list.filter(fn(kv) { { kv.1 }.deleted == False })
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_updates(
  state: State,
  kv: dict.Dict(String, Value),
) -> actor.Next(State, Message) {
  dict.each(kv, fn(k, v) { process.send(state.subject, IncomingUpdate(k, v)) })

  actor.continue(state)
}

fn handle_incoming_update(
  state: State,
  key: String,
  value: Value,
) -> actor.Next(State, Message) {
  kv_store.write(state.store, key, value)

  actor.continue(state)
}

fn start_kv_actor(
  config: KvConfig,
) -> Result(actor.Started(Kv), actor.StartError) {
  let start_result =
    actor.new_with_initialiser(1000, initialize(_, config))
    |> actor.on_message(on_message)
    |> actor.named(config.name)
    |> actor.start

  case start_result {
    Error(error) -> {
      io.println_error("Error starting kv actor")
      echo error
      Error(error)
    }
    Ok(start_result) -> {
      process.link(start_result.pid)
      Ok(start_result)
    }
  }
}

fn get_port(node: swim_store.NodeInfo) -> Int {
  node.kv_port
}

type Request {
  RequestValues(keys: List(String))
}

fn encode_request(req: Request) -> json.Json {
  case req {
    RequestValues(keys:) ->
      json.object([
        #("type", json.string("request_values")),
        #("keys", json.array(keys, json.string)),
      ])
  }
}

fn decode_request() -> decode.Decoder(Request) {
  let request_values_decoder = {
    use keys <- decode.field("keys", decode.list(decode.string))
    decode.success(RequestValues(keys))
  }

  use tag <- decode.field("type", decode.string)
  case tag {
    "request_values" -> request_values_decoder
    _ -> decode.failure(RequestValues([]), "KvRequest")
  }
}

fn request_map(req: Request, response: process.Subject(Response)) -> Message {
  case req {
    RequestValues(keys:) -> GetValues(keys, recv: response)
  }
}

type Response {
  Values(values: dict.Dict(String, Value))
}

fn encode_response(res: Response) -> json.Json {
  case res {
    Values(values:) ->
      json.object([
        #("type", json.string("values")),
        #("values", json.dict(values, function.identity, kv_store.encode_value)),
      ])
  }
}

fn decode_response() -> decode.Decoder(Response) {
  let values_decoder = {
    use values <- decode.field(
      "values",
      decode.dict(decode.string, kv_store.decode_value()),
    )
    decode.success(Values(values:))
  }

  use tag <- decode.field("type", decode.string)
  case tag {
    "values" -> values_decoder
    _ -> decode.failure(Values(dict.new()), "KvResponse")
  }
}

fn response_map(res: Response) -> Message {
  case res {
    Values(values:) -> Updates(values)
  }
}

pub fn supervised(config: KvConfig) {
  supervision.worker(fn() { start_kv_actor(config) })
}

pub fn get(kv: Kv, key: String) {
  process.call(kv.subject, 1000, Get(key, _))
}

pub fn set(kv: Kv, key: String, value: String) {
  process.send(kv.subject, Put(key, value))
}

pub fn delete(kv: Kv, key: String) {
  process.send(kv.subject, Delete(key))
}
