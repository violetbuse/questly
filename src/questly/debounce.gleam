import gleam/bool
import gleam/erlang/process
import gleam/float
import gleam/option
import gleam/otp/actor
import gleam/time/duration
import gleam/time/timestamp

pub opaque type Debouncer(arg_type) {
  Debouncer(subject: process.Subject(Message(arg_type)))
}

pub opaque type Message(arg_type) {
  Internal
  Execute(data: arg_type)
}

type State(arg_type) {
  State(
    subject: process.Subject(Message(arg_type)),
    wait: duration.Duration,
    function: fn(arg_type) -> Nil,
    last_execution: timestamp.Timestamp,
    latest_arg: option.Option(arg_type),
  )
}

fn initialize(
  self: process.Subject(Message(arg_type)),
  milliseconds: Int,
  function: fn(arg_type) -> Nil,
) -> Result(
  actor.Initialised(State(arg_type), Message(arg_type), Debouncer(arg_type)),
  String,
) {
  let return = Debouncer(subject: self)

  let state =
    State(
      subject: self,
      wait: duration.milliseconds(milliseconds),
      function: function,
      last_execution: timestamp.from_unix_seconds(0),
      latest_arg: option.None,
    )

  actor.initialised(state)
  |> actor.returning(return)
  |> Ok
}

fn on_message(
  state: State(arg_type),
  message: Message(arg_type),
) -> actor.Next(State(arg_type), Message(arg_type)) {
  case message {
    Internal -> {
      let now = timestamp.system_time()
      option.map(state.latest_arg, state.function)
      actor.continue(
        State(..state, last_execution: now, latest_arg: option.None),
      )
    }
    Execute(data:) -> {
      let internal_sent = option.is_some(state.latest_arg)
      let new_latest_arg = option.Some(data)

      let _ = {
        use <- bool.guard(when: internal_sent, return: Error(Nil))

        let now = timestamp.system_time() |> timestamp.to_unix_seconds
        let minimum_exec_time =
          timestamp.add(state.last_execution, state.wait)
          |> timestamp.to_unix_seconds

        let time_to_exec =
          { minimum_exec_time -. now }
          |> float.max(0.001)
          |> float.multiply(1000.0)
          |> float.round

        process.send_after(state.subject, time_to_exec, Internal) |> Ok
      }

      actor.continue(State(..state, latest_arg: new_latest_arg))
    }
  }
}

pub fn new(
  wait ms: Int,
  run function: fn(arg_type) -> discard,
) -> Debouncer(arg_type) {
  let internal_function = fn(arg: arg_type) {
    function(arg)
    Nil
  }

  let assert Ok(start_result) =
    actor.new_with_initialiser(1000, initialize(_, ms, internal_function))
    |> actor.on_message(on_message)
    |> actor.start

  process.link(start_result.pid)

  start_result.data
}

pub fn run(debouncer: Debouncer(arg), argument: arg) {
  process.send(debouncer.subject, Execute(argument))
}
