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

pub opaque type Subscriber {
  Subscriber(subject: process.Subject(Message))
}

pub type Filter =
  fn(Event) -> Bool

pub opaque type Message {
  Heartbeat
  Unsubscribe
  ChangeFilter(new_filter: Filter)
  Publish(
    channel: String,
    id: String,
    data: String,
    metadata: dict.Dict(String, String),
  )
  IncomingEvent(Event)
}

type State {
  State(
    pubsub: pubsub.Pubsub,
    receiver: process.Subject(Event),
    sender: process.Subject(Event),
    self: process.Subject(Message),
    selector: process.Selector(Message),
    filter: Filter,
    owning_process: process.Pid,
    already_received: set.Set(Event),
    latest_received: Int,
  )
}

pub fn new(
  pubsub: pubsub.Pubsub,
  client_receiver: process.Subject(Event),
  filter initial: Filter,
  from replay: option.Option(Int),
) -> Result(Subscriber, Nil) {
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
        filter: initial,
        already_received: set.new(),
        latest_received: replay |> option.unwrap(now),
      )

    loop(state)
  })

  process.receive(get_subscriber, 1000)
}

fn loop(state: State) -> State {
  let message = process.selector_receive_forever(state.selector)

  let new_state = case message {
    Heartbeat -> handle_heartbeat(state)
    ChangeFilter(new_filter:) -> handle_change_filter(state, new_filter)
    IncomingEvent(event) -> handle_incoming_event(state, event)
    Publish(channel:, id:, data:, metadata:) ->
      handle_publish(state, channel, id, data, metadata)
    Unsubscribe -> handle_unsubscribe(state)
  }

  loop(new_state)
}

const heartbeat_interval = 30_000

fn handle_heartbeat(state: State) -> State {
  pubsub.subscribe(
    state.pubsub,
    state.receiver,
    state.filter,
    option.Some(state.latest_received),
  )

  process.send_after(state.self, heartbeat_interval, Heartbeat)

  state
}

fn handle_change_filter(state: State, filter: Filter) -> State {
  pubsub.unsubscribe(state.pubsub, state.receiver, state.filter)
  pubsub.subscribe(
    state.pubsub,
    state.receiver,
    filter,
    option.Some(state.latest_received),
  )

  State(..state, filter:)
}

fn handle_incoming_event(state: State, event: Event) -> State {
  let should_send = set.contains(state.already_received, event) |> bool.negate

  case should_send {
    False -> Nil
    True -> process.send(state.sender, event)
  }

  State(
    ..state,
    already_received: set.insert(state.already_received, event),
    latest_received: int.max(state.latest_received, event.time),
  )
}

fn handle_publish(
  state: State,
  channel: String,
  id: String,
  data: String,
  metadata: dict.Dict(String, String),
) -> State {
  let event = pubsub.publish(state.pubsub, channel, id, data, metadata)

  State(..state, already_received: set.insert(state.already_received, event))
}

fn handle_unsubscribe(state: State) -> State {
  pubsub.unsubscribe(state.pubsub, state.receiver, state.filter)

  process.unlink(state.owning_process)
  let assert Ok(pid) = process.subject_owner(state.self)
  process.kill(pid)

  state
}
