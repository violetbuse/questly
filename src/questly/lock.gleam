import gleam/bit_array
import gleam/crypto
import gleam/erlang/process
import gleam/float
import gleam/int
import gleam/list
import gleam/option
import gleam/otp/actor
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import pog
import questly/lock/sql

pub opaque type Lock {
  Lock(subject: process.Subject(Message))
}

pub opaque type Message {
  Heartbeat
  LockLifecycle
  RefreshLockState
  LockResource
  UnlockResource
  GetLockState(recv: process.Subject(LockState))
}

pub type LockState {
  Locked
  Unlocked
}

type State {
  State(
    subject: process.Subject(Message),
    db: process.Name(pog.Message),
    resource: String,
    nonce: String,
    expires_at: option.Option(Int),
    attempting_to_lock: Bool,
    lock_state: LockState,
  )
}

fn initialize(
  self: process.Subject(Message),
  db: process.Name(pog.Message),
  resource: String,
) {
  let initial_state =
    State(
      subject: self,
      db: db,
      resource: resource,
      nonce: crypto.strong_random_bytes(16) |> bit_array.base64_encode(True),
      expires_at: option.None,
      attempting_to_lock: False,
      lock_state: Unlocked,
    )

  let returning = Lock(self)

  process.send_after(self, 120_000, RefreshLockState)

  actor.initialised(initial_state)
  |> actor.returning(returning)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    GetLockState(recv:) -> handle_get_lock_state(state, recv)
    Heartbeat -> handle_heartbeat(state)
    LockLifecycle -> handle_lock_lifecycle(state)
    RefreshLockState -> handle_periodic_lock_state_refresh(state)
    LockResource -> handle_lock_resource(state)
    UnlockResource -> handle_unlock_resource(state)
  }
}

fn handle_get_lock_state(
  state: State,
  recv: process.Subject(LockState),
) -> actor.Next(State, Message) {
  process.send(recv, state.lock_state)

  actor.continue(state)
}

const heartbeat_interval = 28_000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  let now =
    timestamp.system_time()
    |> timestamp.to_unix_seconds
    |> float.round
    |> int.multiply(1000)
  let next_lifecycle_time =
    option.map(state.expires_at, fn(expiry) {
      int.multiply(expiry, 1000)
      |> int.subtract(10_000)
      |> int.max(now)
      |> int.subtract(now)
    })
    |> option.unwrap(0)

  process.send_after(state.subject, next_lifecycle_time, LockLifecycle)
  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  actor.continue(state)
}

const lock_duration_ms = 60_000

fn get_remote_state(
  db: pog.Connection,
  resource: String,
  nonce: String,
) -> #(option.Option(String), option.Option(Int), LockState) {
  let assert Ok(pog.Returned(count: _, rows:)) = sql.query(db, resource)

  let remote_nonce =
    list.first(rows) |> option.from_result |> option.map(fn(row) { row.nonce })

  let expires_at =
    list.first(rows)
    |> option.from_result
    |> option.map(fn(row) { row.expires_at })

  let now = timestamp.system_time() |> timestamp.to_unix_seconds |> float.round

  let locked =
    list.first(rows)
    |> result.map(fn(row) { row.nonce == nonce && row.expires_at > now })
    |> result.unwrap(False)

  let lock_state = case locked {
    False -> Unlocked
    True -> Locked
  }

  #(remote_nonce, expires_at, lock_state)
}

fn handle_lock_lifecycle(state: State) -> actor.Next(State, Message) {
  let db = pog.named_connection(state.db)

  case state.lock_state, state.attempting_to_lock {
    Locked, True -> handle_renew_lock(state, db)
    Locked, False -> handle_release_lock(state, db)
    Unlocked, True -> handle_acquire_lock(state, db)
    Unlocked, False -> handle_refresh_lock_state(state, db)
  }
}

fn handle_renew_lock(
  state: State,
  db: pog.Connection,
) -> actor.Next(State, Message) {
  let lock_until =
    timestamp.system_time()
    |> timestamp.add(duration.milliseconds(lock_duration_ms))
    |> timestamp.to_unix_seconds
    |> float.round

  let assert Ok(_) = sql.renew(db, state.resource, state.nonce, lock_until)

  let #(_, expires_at, lock_state) =
    get_remote_state(db, state.resource, state.nonce)

  actor.continue(State(..state, expires_at:, lock_state:))
}

fn handle_release_lock(
  state: State,
  db: pog.Connection,
) -> actor.Next(State, Message) {
  let assert Ok(_) = sql.release(db, state.resource, state.nonce)

  let #(_, expires_at, lock_state) =
    get_remote_state(db, state.resource, state.nonce)

  actor.continue(State(..state, expires_at:, lock_state:))
}

fn handle_acquire_lock(
  state: State,
  db: pog.Connection,
) -> actor.Next(State, Message) {
  let lock_until =
    timestamp.system_time()
    |> timestamp.add(duration.milliseconds(lock_duration_ms))
    |> timestamp.to_unix_seconds
    |> float.round
  let assert Ok(_) = sql.lock(db, state.resource, state.nonce, lock_until)

  let #(_, expires_at, lock_state) =
    get_remote_state(db, state.resource, state.nonce)

  actor.continue(State(..state, expires_at:, lock_state:))
}

fn handle_refresh_lock_state(
  state: State,
  db: pog.Connection,
) -> actor.Next(State, Message) {
  let #(_remote_nonce, expires_at, lock_state) =
    get_remote_state(db, state.resource, state.nonce)

  actor.continue(State(..state, expires_at:, lock_state:))
}

fn handle_periodic_lock_state_refresh(
  state: State,
) -> actor.Next(State, Message) {
  let #(_, expires_at, lock_state) =
    pog.named_connection(state.db)
    |> get_remote_state(state.resource, state.nonce)

  process.send_after(state.subject, 120_000, RefreshLockState)

  actor.continue(State(..state, expires_at:, lock_state:))
}

fn handle_lock_resource(state: State) -> actor.Next(State, Message) {
  process.send(state.subject, LockLifecycle)

  actor.continue(State(..state, attempting_to_lock: True))
}

fn handle_unlock_resource(state: State) -> actor.Next(State, Message) {
  process.send(state.subject, LockLifecycle)

  actor.continue(State(..state, attempting_to_lock: False))
}

fn start(db: process.Name(pog.Message), resource: String) {
  actor.new_with_initialiser(5000, initialize(_, db, resource))
  |> actor.on_message(on_message)
  |> actor.start
}

pub fn new_locked(db: process.Name(pog.Message), resource: String) -> Lock {
  let assert Ok(start_result) = start(db, resource) as "failed to start lock"
  process.link(start_result.pid)
  process.send(start_result.data.subject, LockResource)

  start_result.data
}

pub fn new_unlocked(db: process.Name(pog.Message), resource: String) -> Lock {
  let assert Ok(start_result) = start(db, resource) as "failed to start lock"
  process.link(start_result.pid)
  process.send(start_result.data.subject, UnlockResource)

  start_result.data
}

pub fn lock(lock: Lock) -> Nil {
  process.send(lock.subject, LockResource)
}

pub fn unlock(lock: Lock) -> Nil {
  process.send(lock.subject, UnlockResource)
}

pub fn is_locked(lock: Lock) -> LockState {
  process.call(lock.subject, 1000, GetLockState)
}
