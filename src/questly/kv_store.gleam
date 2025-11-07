import gleam/dict
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/io
import gleam/json
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/string

pub opaque type KvStore {
  KvStore(subject: process.Subject(Message))
}

pub type Value {
  Value(data: String, version: Int, deleted: Bool)
}

pub fn encode_value(value: Value) -> json.Json {
  json.object([
    #("data", json.string(value.data)),
    #("version", json.int(value.version)),
    #("deleted", json.bool(value.deleted)),
  ])
}

pub fn decode_value() -> decode.Decoder(Value) {
  {
    use data <- decode.field("data", decode.string)
    use version <- decode.field("version", decode.int)
    use deleted <- decode.field("deleted", decode.bool)

    decode.success(Value(data:, version:, deleted:))
  }
}

pub opaque type Message {
  ReadValue(key: String, recv: process.Subject(Result(Value, Nil)))
  ListValues(prefix: String, recv: process.Subject(List(#(String, Value))))
  InsertValue(key: String, value: Value)
  SampleValues(count: Int, recv: process.Subject(List(#(String, Value))))
}

type State {
  State(values: dict.Dict(String, Value))
}

fn initialize(
  self: process.Subject(Message),
) -> Result(actor.Initialised(State, Message, KvStore), String) {
  let return = KvStore(self)

  let state = State(values: dict.new())

  actor.initialised(state)
  |> actor.returning(return)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    InsertValue(key:, value:) -> handle_insert_value(state, key, value)
    ListValues(prefix:, recv:) -> handle_list_values(state, prefix, recv)
    ReadValue(key:, recv:) -> handle_read_value(state, key, recv)
    SampleValues(count:, recv:) -> handle_sample_values(state, count, recv)
  }
}

fn handle_insert_value(
  state: State,
  key: String,
  value: Value,
) -> actor.Next(State, Message) {
  let new_values =
    dict.upsert(state.values, key, fn(existing) {
      case existing {
        option.None -> value
        option.Some(existing) if value.version > existing.version -> value
        option.Some(existing) if value.version < existing.version -> existing
        option.Some(_existing) if value.deleted == True -> value
        option.Some(existing) -> existing
      }
    })

  actor.continue(State(values: new_values))
}

fn handle_list_values(
  state: State,
  prefix: String,
  recv: process.Subject(List(#(String, Value))),
) -> actor.Next(State, Message) {
  dict.filter(state.values, fn(key, _) { string.starts_with(key, prefix) })
  |> dict.to_list()
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_read_value(
  state: State,
  key: String,
  recv: process.Subject(Result(Value, Nil)),
) -> actor.Next(State, Message) {
  dict.get(state.values, key)
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_sample_values(
  state: State,
  count: Int,
  recv: process.Subject(List(#(String, Value))),
) -> actor.Next(State, Message) {
  dict.to_list(state.values)
  |> list.sample(count)
  |> process.send(recv, _)

  actor.continue(state)
}

pub fn new() -> Result(KvStore, Nil) {
  let start_result =
    actor.new_with_initialiser(500, initialize)
    |> actor.on_message(on_message)
    |> actor.start

  case start_result {
    Error(error) -> {
      io.println_error("Error starting kv store")
      echo error
      Error(Nil)
    }
    Ok(start_result) -> {
      process.link(start_result.pid)
      Ok(start_result.data)
    }
  }
}

pub fn read(store: KvStore, key: String) -> Result(Value, Nil) {
  process.call(store.subject, 1000, ReadValue(key, _))
}

pub fn list(store: KvStore, prefix: String) {
  process.call(store.subject, 1000, ListValues(prefix, _))
}

pub fn write(store: KvStore, key: String, value: Value) {
  process.send(store.subject, InsertValue(key, value))
}

pub fn sample(store: KvStore, count: Int) {
  process.call(store.subject, 1000, SampleValues(count, _))
}
