import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/float
import gleam/function
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/order
import gleam/otp/actor
import gleam/result
import gleam/time/timestamp

pub type PubsubStore {
  PubsubStore(subject: process.Subject(Message))
}

pub type Event {
  Event(
    channel: String,
    id: String,
    data: String,
    metadata: dict.Dict(String, String),
    retain_until: Int,
    time: Int,
  )
}

pub fn compare_events(event_1: Event, event_2: Event) -> order.Order {
  int.compare(event_1.time, event_2.time)
}

pub fn encode_event(event: Event) -> json.Json {
  json.object([
    #("channel", json.string(event.channel)),
    #("id", json.string(event.id)),
    #("data", json.string(event.data)),
    #("metadata", json.dict(event.metadata, function.identity, json.string)),
    #("retain_until", json.int(event.retain_until)),
    #("time", json.int(event.time)),
  ])
}

pub fn decode_event() -> decode.Decoder(Event) {
  {
    use channel <- decode.field("channel", decode.string)
    use id <- decode.field("id", decode.string)
    use data <- decode.field("data", decode.string)
    use metadata <- decode.field(
      "metadata",
      decode.dict(decode.string, decode.string),
    )
    use retain_until <- decode.field("retain_until", decode.int)
    use time <- decode.field("time", decode.int)

    decode.success(Event(channel:, id:, data:, metadata:, retain_until:, time:))
  }
}

pub opaque type Message {
  Heartbeat
  ListNodes(recv: process.Subject(List(String)))
  GetLatest(node_id: String, recv: process.Subject(Result(Event, Nil)))
  GetFrom(node_id: String, since: Int, recv: process.Subject(List(Event)))
  Insert(
    node_id: String,
    events: List(Event),
    recv: process.Subject(List(Event)),
  )
}

type State {
  State(events: dict.Dict(String, List(Event)), self: process.Subject(Message))
}

fn initialize(
  self: process.Subject(Message),
) -> Result(actor.Initialised(State, Message, PubsubStore), String) {
  let return = PubsubStore(self)

  let state = State(events: dict.new(), self:)

  process.send(self, Heartbeat)

  actor.initialised(state)
  |> actor.returning(return)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    Heartbeat -> handle_heartbeat(state)
    GetFrom(node_id:, since:, recv:) ->
      handle_get_from(state, node_id, since, recv)
    GetLatest(node_id:, recv:) -> handle_get_latest(state, node_id, recv)
    Insert(node_id:, events:, recv:) ->
      handle_insert(state, node_id, events, recv)
    ListNodes(recv:) -> handle_list_nodes(state, recv)
  }
}

const heartbeat_interval = 60_000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.send_after(state.self, heartbeat_interval, Heartbeat)

  let now = timestamp.system_time() |> timestamp.to_unix_seconds |> float.round

  let new_events =
    dict.map_values(state.events, fn(_, events) {
      list.filter(events, fn(event) { event.retain_until > now })
    })

  actor.continue(State(..state, events: new_events))
}

fn handle_list_nodes(
  state: State,
  recv: process.Subject(List(String)),
) -> actor.Next(State, Message) {
  dict.keys(state.events)
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_get_latest(
  state: State,
  node_id: String,
  recv: process.Subject(Result(Event, Nil)),
) -> actor.Next(State, Message) {
  let now = timestamp.system_time() |> timestamp.to_unix_seconds |> float.round

  dict.get(state.events, node_id)
  |> result.map(list.last)
  |> result.flatten
  |> result.try(fn(event) {
    case event.retain_until > now {
      False -> Error(Nil)
      True -> Ok(event)
    }
  })
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_get_from(
  state: State,
  node_id: String,
  since: Int,
  recv: process.Subject(List(Event)),
) -> actor.Next(State, Message) {
  let now = timestamp.system_time() |> timestamp.to_unix_seconds |> float.round

  dict.get(state.events, node_id)
  |> result.map(list.filter(_, fn(event) { event.time > since }))
  |> result.unwrap([])
  |> list.filter(fn(event) { event.retain_until > now })
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_insert(
  state: State,
  node_id: String,
  events: List(Event),
  recv: process.Subject(List(Event)),
) -> actor.Next(State, Message) {
  let existing_events = dict.get(state.events, node_id) |> result.unwrap([])
  let latest_time =
    list.last(existing_events)
    |> result.map(fn(event) { event.time })
    |> result.unwrap(0)

  let to_insert =
    list.filter(events, fn(event) { event.time > latest_time })
    |> list.sort(compare_events)

  let new_events = list.append(existing_events, to_insert)
  process.send(recv, new_events)

  let new_dict = dict.insert(state.events, node_id, new_events)

  actor.continue(State(..state, events: new_dict))
}

pub fn new() -> Result(PubsubStore, Nil) {
  let start_result =
    actor.new_with_initialiser(500, initialize)
    |> actor.on_message(on_message)
    |> actor.start

  case start_result {
    Error(error) -> {
      io.println_error("Error starting pubsub store")
      echo error
      Error(Nil)
    }
    Ok(start_result) -> {
      process.link(start_result.pid)
      Ok(start_result.data)
    }
  }
}

pub fn list_nodes(store: PubsubStore) {
  process.call(store.subject, 1000, ListNodes)
}

pub fn get_latest(store: PubsubStore, node_id: String) {
  process.call(store.subject, 1000, GetLatest(node_id, _))
}

pub fn get_from(store: PubsubStore, node_id: String, since: Int) {
  process.call(store.subject, 1000, GetFrom(node_id, since, _))
}

/// returns the newly inserted events
pub fn insert(store: PubsubStore, node_id: String, events: List(Event)) {
  process.call(store.subject, 1000, Insert(node_id, events, _))
}
