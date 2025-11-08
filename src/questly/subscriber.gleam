import gleam/bool
import gleam/dict
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/option
import gleam/set
import gleam/time/timestamp
import questly/pubsub
import questly/pubsub_store.{type Event}

pub opaque type Subscriber(output) {
  Subscriber(subject: process.Subject(Message(output)))
}

pub type Mapper(output) =
  fn(Event) -> Result(output, Nil)

pub opaque type Message(output) {
  Heartbeat
  Unsubscribe
  ChangeMap(new_map: Mapper(output))
  Publish(
    channel: String,
    id: String,
    data: String,
    metadata: dict.Dict(String, String),
    retain_until: Int,
  )
  IncomingEvent(Event)
}

type State(output) {
  State(
    pubsub: pubsub.Pubsub,
    receiver: process.Subject(Event),
    sender: process.Subject(output),
    self: process.Subject(Message(output)),
    selector: process.Selector(Message(output)),
    mapper: Mapper(output),
    owning_process: process.Pid,
    already_received: set.Set(Event),
    latest_received: Int,
  )
}

pub fn new(
  pubsub: pubsub.Pubsub,
  recv client_receiver: process.Subject(output),
  map initial: Mapper(output),
  from replay: option.Option(Int),
) -> Subscriber(output) {
  let get_subscriber = process.new_subject()
  let owning_process = process.self()

  process.spawn(fn() {
    let self = process.new_subject()
    let receiver = process.new_subject()

    let subscriber = Subscriber(self)

    process.send(get_subscriber, subscriber)
    process.send(self, Heartbeat)

    let selector =
      process.new_selector()
      |> process.select(self)
      |> process.select_map(receiver, IncomingEvent)

    let now =
      timestamp.system_time() |> timestamp.to_unix_seconds |> float.round

    let state =
      State(
        pubsub:,
        receiver:,
        sender: client_receiver,
        self:,
        selector:,
        owning_process:,
        mapper: initial,
        already_received: set.new(),
        latest_received: replay |> option.unwrap(now),
      )

    loop(state)
  })

  let assert Ok(subscriber) = process.receive(get_subscriber, 1000)
  subscriber
}

fn loop(state: State(output)) -> State(output) {
  let message = process.selector_receive_forever(state.selector)

  let new_state = case message {
    Heartbeat -> handle_heartbeat(state)
    ChangeMap(new_map:) -> handle_change_mapper(state, new_map)
    IncomingEvent(event) -> handle_incoming_event(state, event)
    Publish(channel:, id:, data:, metadata:, retain_until:) ->
      handle_publish(state, channel, id, data, metadata, retain_until)
    Unsubscribe -> handle_unsubscribe(state)
  }

  loop(new_state)
}

fn filter_any(_event: Event) -> Bool {
  True
}

const heartbeat_interval = 30_000

fn handle_heartbeat(state: State(output)) -> State(output) {
  pubsub.subscribe(
    state.pubsub,
    state.receiver,
    filter_any,
    option.Some(state.latest_received),
  )

  process.send_after(state.self, heartbeat_interval, Heartbeat)

  state
}

fn handle_change_mapper(
  state: State(output),
  mapper: Mapper(output),
) -> State(output) {
  State(..state, mapper:)
}

fn handle_incoming_event(state: State(output), event: Event) -> State(output) {
  let _ = {
    use <- bool.guard(
      when: set.contains(state.already_received, event),
      return: Nil,
    )

    case state.mapper(event) {
      Error(_) -> Nil
      Ok(mapped) -> {
        process.send(state.sender, mapped)
        Nil
      }
    }
  }

  State(
    ..state,
    already_received: set.insert(state.already_received, event),
    latest_received: int.max(state.latest_received, event.time),
  )
}

fn handle_publish(
  state: State(output),
  channel: String,
  id: String,
  data: String,
  metadata: dict.Dict(String, String),
  retain_until: Int,
) -> State(output) {
  let event =
    pubsub.publish(state.pubsub, channel, id, data, metadata, retain_until)

  State(..state, already_received: set.insert(state.already_received, event))
}

fn handle_unsubscribe(state: State(output)) -> State(output) {
  pubsub.unsubscribe(state.pubsub, state.receiver, filter_any)

  process.unlink(state.owning_process)
  let assert Ok(pid) = process.subject_owner(state.self)
  process.kill(pid)

  state
}

pub fn change_map(subscriber: Subscriber(output), mapper: Mapper(output)) {
  process.send(subscriber.subject, ChangeMap(mapper))
}

pub fn close(subscriber: Subscriber(output)) {
  process.send(subscriber.subject, Unsubscribe)
}

pub fn publish(
  subscriber: Subscriber(output),
  channel: String,
  id: String,
  data: String,
  metadata: dict.Dict(String, String),
  retain_until: Int,
) {
  process.send(
    subscriber.subject,
    Publish(channel:, id:, data:, metadata:, retain_until:),
  )
}
