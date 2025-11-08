import gleam/bool
import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/function
import gleam/http
import gleam/http/request
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/string
import gleam/time/timestamp
import httpp/send
import mist
import questly/kv_store.{type Value, Value}
import questly/swim
import questly/swim_store
import questly/util
import wisp
import wisp/wisp_mist

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
  GetValues(keys: List(String), recv: process.Subject(dict.Dict(String, Value)))
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
    swim: swim.Swim,
    port: Int,
    secret: String,
  )
}

fn initialize(
  self: process.Subject(Message),
  config: KvConfig,
) -> Result(actor.Initialised(State, Message, Kv), String) {
  let result = Kv(self)

  let assert Ok(kv_store) = kv_store.new() as "kv could not start kv store"

  let state =
    State(
      store: kv_store,
      subject: self,
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
    AnnounceTimes(from:, time_map:) ->
      handle_announce_times(state, from, time_map)
    GetValues(keys:, recv:) -> handle_get_values(state, keys, recv)
    Delete(key:) -> handle_delete(state, key)
    Get(key:, recv:) -> handle_get(state, key, recv)
    IncomingUpdate(key:, value:) -> handle_incoming_update(state, key, value)
    List(prefix:, recv:) -> handle_list(state, prefix, recv)
    Put(key:, data:) -> handle_put(state, key, data)
  }
}

const heartbeat_interval = 10_000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  process.spawn(fn() {
    let self = swim.get_self(state.swim)
    let candidates =
      swim.get_remote(state.swim)
      |> list.filter(swim_store.is_alive)
      |> list.sample(3)

    list.each(candidates, fn(node) {
      process.spawn(fn() {
        let to_sync =
          kv_store.sample(state.store, 7)
          |> list.map(fn(kv) { #(kv.0, { kv.1 }.version) })
          |> dict.from_list

        send_time_announcement(
          host: node.hostname,
          port: state.port,
          secret: state.secret,
          from: self.id,
          announce: to_sync,
        )
      })
    })
  })

  actor.continue(state)
}

const anti_entropy_sync_interval = 60_000

fn handle_anti_entropy_sync(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, anti_entropy_sync_interval, AntiEntropySync)

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

    let host =
      swim.get_remote(state.swim)
      |> list.find(fn(node) { node.id == from })
      |> result.map(fn(node) { node.hostname })

    use hostname <- result.try(host)

    use values <- result.try(send_value_request(
      host: hostname,
      port: state.port,
      secret: state.secret,
      keys:,
    ))

    dict.each(values, fn(k, v) {
      process.send(state.subject, IncomingUpdate(k, v))
    })

    Ok(Nil)
  })

  actor.continue(state)
}

fn handle_get_values(
  state: State,
  keys: List(String),
  recv: process.Subject(dict.Dict(String, Value)),
) -> actor.Next(State, Message) {
  kv_store.list(state.store, "")
  |> list.filter(fn(kv) { list.contains(keys, kv.0) })
  |> dict.from_list
  |> process.send(recv, _)

  actor.continue(state)
}

fn broadcast_new_version(state: State, key: String, new_time: Int) {
  process.spawn(fn() {
    let dict = [#(key, new_time)] |> dict.from_list

    let self = swim.get_self(state.swim)
    let remote = swim.get_remote(state.swim) |> list.filter(swim_store.is_alive)

    list.each(remote, fn(node) {
      process.spawn(fn() {
        send_time_announcement(
          host: node.hostname,
          port: state.port,
          secret: state.secret,
          from: self.id,
          announce: dict,
        )
      })
    })
  })
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

  broadcast_new_version(state, key, new_value.version)

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

  broadcast_new_version(state, key, new_value.version)

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

type Context {
  Context(kv: Kv)
}

fn router(req: wisp.Request, context: Context) -> wisp.Response {
  case wisp.path_segments(req) {
    ["health"] -> handle_api_health_check(context)
    ["values"] -> handle_value_request(req, context)
    ["announce_times"] -> handle_time_announcement(req, context)
    _ -> wisp.not_found()
  }
}

fn handle_api_health_check(context: Context) -> wisp.Response {
  process.call(context.kv.subject, 1000, HealthCheck)

  wisp.ok()
}

fn handle_value_request(req: wisp.Request, context: Context) -> wisp.Response {
  let keys =
    wisp.get_query(req)
    |> list.key_find("keys")
    |> result.unwrap("")
    |> string.split(",")
    |> list.map(string.trim)
    |> list.filter(fn(str) { string.is_empty(str) |> bool.negate })

  let values = process.call(context.kv.subject, 1000, GetValues(keys, _))

  json.dict(values, function.identity, kv_store.encode_value)
  |> json.to_string
  |> wisp.json_response(200)
}

fn send_value_request(
  host hostname: String,
  port port: Int,
  secret key: String,
  keys keys: List(String),
) -> Result(dict.Dict(String, Value), Nil) {
  let keys_list = string.join(keys, ",")

  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, "/values"))
    |> result.map(
      request.set_query(_, [#("secret", key), #("keys", keys_list)]),
    )
    as "could not create value request request"

  let server_response =
    send.send(request)
    |> result.map_error(fn(err) {
      io.println_error("Error sending value request to" <> hostname)
      echo err
    })
    |> result.replace_error(Nil)

  use response <- result.try(server_response)

  json.parse(response.body, decode.dict(decode.string, kv_store.decode_value()))
  |> result.replace_error(Nil)
}

fn handle_time_announcement(
  req: wisp.Request,
  context: Context,
) -> wisp.Response {
  use <- wisp.require_method(req, http.Post)
  use <- wisp.require_content_type(req, "application/json")
  use json <- wisp.require_json(req)

  let decoder = {
    use from <- decode.field("from", decode.string)
    use time_map <- decode.field(
      "time_map",
      decode.dict(decode.string, decode.int),
    )

    decode.success(AnnounceTimes(from:, time_map:))
  }

  let decode = decode.run(json, decoder)

  case decode {
    Error(_) ->
      json.object([#("error", json.string("Malformed request"))])
      |> json.to_string
      |> wisp.json_response(400)
    Ok(message) -> {
      process.send(context.kv.subject, message)

      wisp.ok()
    }
  }
}

fn send_time_announcement(
  host hostname: String,
  port port: Int,
  secret key: String,
  from node: String,
  announce times: dict.Dict(String, Int),
) -> Result(Nil, Nil) {
  let body =
    json.object([
      #("from", json.string(node)),
      #("time_map", json.dict(times, function.identity, json.int)),
    ])
    |> json.to_string

  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, "/announce_times"))
    |> result.map(request.set_method(_, http.Post))
    |> result.map(request.set_header(_, "content-type", "application/json"))
    |> result.map(request.set_query(_, [#("secret", key)]))
    |> result.map(request.set_body(_, body))
    as "could not create event announcement request"

  send.send(request)
  |> result.map_error(fn(err) {
    io.println_error("error sending event announcement to " <> hostname)
    echo err
  })
  |> result.replace_error(Nil)
  |> result.replace(Nil)
}

fn start_kv_api(kv: Kv, config: KvConfig) {
  wisp.configure_logger()

  let context = Context(kv:)
  let start_result =
    wisp_mist.handler(router(_, context), config.secret)
    |> mist.new
    |> mist.bind("0.0.0.0")
    |> mist.with_ipv6
    |> mist.port(config.port)
    |> mist.start

  start_result
  |> result.replace_error(actor.InitFailed("Failed to start kv api"))
}

pub fn supervised(config: KvConfig) {
  supervision.worker(fn() {
    use actor.Started(_, kv) as start_result <- result.try(start_kv_actor(
      config,
    ))

    use actor.Started(api_pid, _) <- result.try(start_kv_api(kv, config))

    process.send(kv.subject, LinkApi(api_pid))

    Ok(start_result)
  })
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
