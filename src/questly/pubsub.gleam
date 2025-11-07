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
import gleam/option
import gleam/otp/actor
import gleam/otp/supervision
import gleam/result
import gleam/set
import gleam/string
import gleam/time/timestamp
import httpp/send
import mist
import questly/pubsub_store.{type Event, Event}
import questly/swim
import questly/swim_store
import questly/util
import wisp
import wisp/wisp_mist

pub opaque type Pubsub {
  Pubsub(subject: process.Subject(Message))
}

pub fn from_name(name: process.Name(Message)) {
  Pubsub(process.named_subject(name))
}

pub type PubsubConfig {
  PubsubConfig(
    name: process.Name(Message),
    swim: swim.Swim,
    port: Int,
    secret: String,
  )
}

pub type Subscription =
  #(process.Subject(Event), fn(Event) -> Bool)

pub opaque type Message {
  Heartbeat
  HealthCheck(recv: process.Subject(Nil))
  LinkApi(pid: process.Pid)
  GetFrom(node_id: String, from: Int, recv: process.Subject(List(Event)))
  Insert(node_id: String, events: List(Event))
  AnnounceEvents(from_node: String, events: dict.Dict(String, Int))
  Publish(
    channel: String,
    id: String,
    data: String,
    metadata: dict.Dict(String, String),
    recv: process.Subject(Event),
  )
  Subscribe(subscription: Subscription, replay_from: option.Option(Int))
  Unsubscribe(subscription: Subscription)
  ReplayEvents(
    since: Int,
    filter: fn(Event) -> Bool,
    recv: process.Subject(List(Event)),
  )
}

type State {
  State(
    store: pubsub_store.PubsubStore,
    subscribers: set.Set(Subscription),
    subject: process.Subject(Message),
    swim: swim.Swim,
    port: Int,
    secret: String,
  )
}

fn initialize(
  self: process.Subject(Message),
  config: PubsubConfig,
) -> Result(actor.Initialised(State, Message, Pubsub), String) {
  let result = Pubsub(self)

  let assert Ok(pubsub_store) = pubsub_store.new()
    as "pubsub could not start pubsub store"

  let state =
    State(
      store: pubsub_store,
      subscribers: set.new(),
      subject: self,
      swim: config.swim,
      port: config.port,
      secret: config.secret,
    )

  process.send(self, Heartbeat)

  actor.initialised(state)
  |> actor.returning(result)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    AnnounceEvents(from_node:, events:) ->
      handle_announce_events(state, from_node, events)
    GetFrom(node_id:, from:, recv:) ->
      handle_get_from(state, node_id, from, recv)
    Insert(node_id:, events:) -> handle_insert(state, node_id, events)
    Heartbeat -> handle_heartbeat(state)
    LinkApi(pid:) -> handle_link_api(state, pid)
    Publish(channel:, id:, data:, metadata:, recv:) ->
      handle_publish(state, channel, id, data, metadata, recv)
    Subscribe(subcription, replay_from) ->
      handle_subscribe(state, subcription, replay_from)
    Unsubscribe(subscription) -> handle_unsubscribe(state, subscription)
    ReplayEvents(since:, filter:, recv:) ->
      handle_replay_events(state, since, filter, recv)
    HealthCheck(recv:) -> handle_health_check(state, recv)
  }
}

const heartbeat_interval = 5000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  process.spawn(fn() {
    let self = swim.get_self(state.swim)
    let remote_nodes =
      swim.get_remote(state.swim)
      |> list.filter(swim_store.is_alive)
      |> list.sample(3)

    let storage_nodes = pubsub_store.list_nodes(state.store)

    list.each(remote_nodes, fn(node) {
      let to_announce =
        list.sample(storage_nodes, 3)
        |> list.map(fn(node_id) {
          let latest =
            pubsub_store.get_latest(state.store, node_id)
            |> result.map(fn(event) { event.time })
            |> result.unwrap(0)
          #(node_id, latest)
        })
        |> dict.from_list

      send_event_announcement(
        host: node.hostname,
        port: state.port,
        secret: state.secret,
        from: self.id,
        announce: to_announce,
      )
    })
  })

  actor.continue(state)
}

fn handle_health_check(
  state: State,
  recv: process.Subject(Nil),
) -> actor.Next(State, Message) {
  process.send(recv, Nil)
  actor.continue(state)
}

fn handle_announce_events(
  state: State,
  from_node: String,
  events: dict.Dict(String, Int),
) -> actor.Next(State, Message) {
  process.spawn(fn() {
    let remote_nodes = swim.get_remote(state.swim)
    let remote_node = list.find(remote_nodes, fn(node) { node.id == from_node })
    use node <- result.try(remote_node)

    dict.each(events, fn(node_id, remote_latest) {
      process.spawn(fn() {
        let local_latest =
          pubsub_store.get_latest(state.store, node_id)
          |> result.map(fn(event) { event.time })
          |> result.unwrap(0)

        use <- bool.guard(when: local_latest >= remote_latest, return: Ok(Nil))

        let event_request =
          send_event_request(
            host: node.hostname,
            port: state.port,
            secret: state.secret,
            for_node: node_id,
            from: local_latest,
          )

        case event_request {
          Error(_) -> Error(Nil)
          Ok(new_events) -> {
            process.send(state.subject, Insert(node_id:, events: new_events))
            Ok(Nil)
          }
        }
      })
    })

    Ok(Nil)
  })

  actor.continue(state)
}

fn handle_link_api(state: State, pid: process.Pid) -> actor.Next(State, Message) {
  process.link(pid)

  actor.continue(state)
}

fn handle_get_from(
  state: State,
  node_id: String,
  from: Int,
  recv: process.Subject(List(Event)),
) -> actor.Next(State, Message) {
  pubsub_store.get_from(state.store, node_id, from)
  |> process.send(recv, _)

  actor.continue(state)
}

fn broadcast(events: List(Event), state: State) {
  process.spawn(fn() {
    list.each(events, fn(event) {
      set.each(state.subscribers, fn(sub) {
        let #(recv, filter) = sub
        case filter(event) {
          False -> Nil
          True -> process.send(recv, event)
        }
      })
    })
  })
}

fn handle_insert(
  state: State,
  node_id: String,
  events: List(Event),
) -> actor.Next(State, Message) {
  let inserted = pubsub_store.insert(state.store, node_id, events)

  broadcast(inserted, state)

  actor.continue(state)
}

fn handle_publish(
  state: State,
  channel: String,
  id: String,
  data: String,
  metadata: dict.Dict(String, String),
  recv: process.Subject(Event),
) -> actor.Next(State, Message) {
  let self_node_id = swim.get_self(state.swim).id
  let last_time =
    pubsub_store.get_latest(state.store, self_node_id)
    |> result.map(fn(event) { event.time })
    |> result.unwrap(0)
  let current_time =
    timestamp.system_time() |> timestamp.to_unix_seconds |> float.round
  let time = int.max(current_time, last_time + 1)

  let new_event = Event(channel:, id:, data:, metadata:, time:)
  let inserted = pubsub_store.insert(state.store, self_node_id, [new_event])

  broadcast(inserted, state)

  process.send(recv, new_event)

  process.spawn(fn() {
    let alive_nodes =
      swim.get_remote(state.swim) |> list.filter(swim_store.is_alive)
    let dict = [#(self_node_id, new_event.time)] |> dict.from_list

    list.each(alive_nodes, fn(node) {
      process.spawn(fn() {
        send_event_announcement(
          host: node.hostname,
          port: state.port,
          secret: state.secret,
          from: self_node_id,
          announce: dict,
        )
      })
    })
  })

  actor.continue(state)
}

fn handle_subscribe(
  state: State,
  subscription: Subscription,
  replay_from: option.Option(Int),
) -> actor.Next(State, Message) {
  process.spawn(fn() {
    let #(receiver, filter) = subscription

    let now =
      timestamp.system_time() |> timestamp.to_unix_seconds |> float.round

    let replay_from = option.unwrap(replay_from, now)

    let events =
      pubsub_store.list_nodes(state.store)
      |> list.map(pubsub_store.get_from(state.store, _, replay_from))
      |> list.flatten
      |> list.sort(pubsub_store.compare_events)

    list.each(events, fn(event) {
      case filter(event) {
        False -> Nil
        True -> process.send(receiver, event)
      }
    })
  })

  actor.continue(
    State(..state, subscribers: set.insert(state.subscribers, subscription)),
  )
}

fn handle_unsubscribe(
  state: State,
  subscription: Subscription,
) -> actor.Next(State, Message) {
  actor.continue(
    State(..state, subscribers: set.delete(state.subscribers, subscription)),
  )
}

fn handle_replay_events(
  state: State,
  since: Int,
  filter: fn(Event) -> Bool,
  recv: process.Subject(List(Event)),
) -> actor.Next(State, Message) {
  pubsub_store.list_nodes(state.store)
  |> list.map(pubsub_store.get_from(state.store, _, since))
  |> list.flatten
  |> list.filter(filter)
  |> list.sort(pubsub_store.compare_events)
  |> process.send(recv, _)

  actor.continue(state)
}

fn start_pubsub_actor(
  config: PubsubConfig,
) -> Result(actor.Started(Pubsub), actor.StartError) {
  let start_result =
    actor.new_with_initialiser(1000, initialize(_, config))
    |> actor.on_message(on_message)
    |> actor.named(config.name)
    |> actor.start

  case start_result {
    Error(error) -> {
      io.println_error("Error starting pubsub actor")
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
  Context(pubsub: Pubsub)
}

fn router(req: wisp.Request, context: Context) -> wisp.Response {
  case wisp.path_segments(req) {
    ["health"] -> handle_api_health_check(context)
    ["events", node_id, from] -> handle_event_request(node_id, from, context)
    ["announce_events"] -> handle_event_announcement(req, context)
    _ -> wisp.not_found()
  }
}

fn handle_api_health_check(context: Context) -> wisp.Response {
  process.call(context.pubsub.subject, 1000, HealthCheck)
  wisp.ok()
}

fn handle_event_request(
  node_id: String,
  from: String,
  context: Context,
) -> wisp.Response {
  let malformed = wisp.response(400)
  let from_parsed = int.parse(from)
  use <- bool.guard(when: result.is_error(from_parsed), return: malformed)
  let assert Ok(from) = from_parsed

  let events =
    process.call(context.pubsub.subject, 1000, GetFrom(node_id, from, _))
  let data = json.array(events, pubsub_store.encode_event) |> json.to_string

  wisp.json_response(data, 200)
}

fn send_event_request(
  host hostname: String,
  port port: Int,
  secret key: String,
  for_node id: String,
  from time: Int,
) -> Result(List(Event), Nil) {
  let path = string.join(["/events", id, int.to_string(time)], "/")
  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, path))
    |> result.map(request.set_query(_, [#("secret", key)]))
    as "could not create event request request"

  let server_response =
    send.send(request)
    |> result.map_error(fn(err) {
      io.println_error("Error sending event request to" <> hostname)
      echo err
    })
    |> result.replace_error(Nil)

  use response <- result.try(server_response)

  json.parse(response.body, decode.list(pubsub_store.decode_event()))
  |> result.replace_error(Nil)
}

fn handle_event_announcement(
  req: wisp.Request,
  context: Context,
) -> wisp.Response {
  use <- wisp.require_method(req, http.Post)
  use <- wisp.require_content_type(req, "application/json")
  use json <- wisp.require_json(req)

  let decoder = {
    use from <- decode.field("from", decode.string)
    use events <- decode.field("events", decode.dict(decode.string, decode.int))

    decode.success(AnnounceEvents(from_node: from, events:))
  }

  let decode = decode.run(json, decoder)

  case decode {
    Error(_) ->
      json.object([#("error", json.string("Malformed request"))])
      |> json.to_string
      |> wisp.json_response(400)
    Ok(message) -> {
      process.send(context.pubsub.subject, message)

      wisp.ok()
    }
  }
}

fn send_event_announcement(
  host hostname: String,
  port port: Int,
  secret key: String,
  from node: String,
  announce events: dict.Dict(String, Int),
) -> Result(Nil, Nil) {
  let body =
    json.object([
      #("from", json.string(node)),
      #("events", json.dict(events, function.identity, json.int)),
    ])
    |> json.to_string

  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, "/announce_events"))
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

fn start_pubsub_api(pubsub: Pubsub, config: PubsubConfig) {
  wisp.configure_logger()

  let context = Context(pubsub:)
  let start_result =
    wisp_mist.handler(router(_, context), config.secret)
    |> mist.new
    |> mist.bind("0.0.0.0")
    |> mist.with_ipv6
    |> mist.port(config.port)
    |> mist.start

  start_result
  |> result.replace_error(actor.InitFailed("Failed to start pubsub api"))
}

pub fn supervised(config: PubsubConfig) {
  supervision.worker(fn() {
    use actor.Started(_, pubsub) as start_result <- result.try(
      start_pubsub_actor(config),
    )

    use actor.Started(api_pid, _) <- result.try(start_pubsub_api(pubsub, config))

    process.send(pubsub.subject, LinkApi(api_pid))

    Ok(start_result)
  })
}

pub fn publish(
  pubsub: Pubsub,
  channel: String,
  id: String,
  data: String,
  metadata: dict.Dict(String, String),
) -> Event {
  process.call(pubsub.subject, 1000, Publish(channel, id, data, metadata, _))
}

pub fn subscribe(
  pubsub: Pubsub,
  receiver: process.Subject(Event),
  filter: fn(Event) -> Bool,
  replay_from: option.Option(Int),
) -> Nil {
  process.send(pubsub.subject, Subscribe(#(receiver, filter), replay_from))
}

pub fn unsubscribe(
  pubsub: Pubsub,
  receiver: process.Subject(Event),
  filter: fn(Event) -> Bool,
) -> Nil {
  process.send(pubsub.subject, Unsubscribe(#(receiver, filter)))
}

pub fn replay_events(
  pubsub: Pubsub,
  since: Int,
  filter: fn(Event) -> Bool,
) -> List(Event) {
  process.call(pubsub.subject, 1000, ReplayEvents(since, filter, _))
}
