import gleam/bool
import gleam/dynamic/decode
import gleam/erlang/process
import gleam/int
import gleam/io
import gleam/json
import gleam/list
import gleam/order
import gleam/otp/actor
import gleam/string

pub type SwimStore {
  SwimStore(subject: process.Subject(Message))
}

pub type NodeInfo {
  NodeInfo(
    id: String,
    hash: Int,
    version: Int,
    state: NodeState,
    hostname: String,
    swim_port: Int,
    region: String,
    kv_port: Int,
    pubsub_port: Int,
  )
}

pub fn compare_node(node_1: NodeInfo, node_2: NodeInfo) -> order.Order {
  let state_to_int = fn(state: NodeState) {
    case state {
      Alive -> 1
      Suspect -> 2
      Dead -> 3
    }
  }

  let state_order =
    int.compare(state_to_int(node_1.state), state_to_int(node_2.state))
  let region_order = string.compare(node_1.region, node_2.region)
  let id_order = string.compare(node_1.id, node_2.id)

  state_order
  |> order.break_tie(region_order)
  |> order.break_tie(id_order)
}

pub fn is_alive(node: NodeInfo) {
  node.state == Alive
}

pub fn is_suspect(node: NodeInfo) {
  node.state == Suspect
}

pub fn is_dead(node: NodeInfo) {
  node.state == Dead
}

pub fn encode_node_info(info: NodeInfo) -> json.Json {
  json.object([
    #("id", json.string(info.id)),
    #("hash", json.int(info.hash)),
    #("version", json.int(info.version)),
    #("state", encode_node_state(info.state)),
    #("hostname", json.string(info.hostname)),
    #("swim_port", json.int(info.swim_port)),
    #("region", json.string(info.region)),
    #("kv_port", json.int(info.kv_port)),
    #("pubsub_port", json.int(info.pubsub_port)),
  ])
}

pub fn decode_node_info() -> decode.Decoder(NodeInfo) {
  {
    use id <- decode.field("id", decode.string)
    use hash <- decode.field("hash", decode.int)
    use version <- decode.field("version", decode.int)
    use state <- decode.field("state", decode_node_state())
    use hostname <- decode.field("hostname", decode.string)
    use swim_port <- decode.field("swim_port", decode.int)
    use region <- decode.field("region", decode.string)
    use kv_port <- decode.field("kv_port", decode.int)
    use pubsub_port <- decode.field("pubsub_port", decode.int)

    decode.success(NodeInfo(
      id:,
      hash:,
      version:,
      state:,
      hostname:,
      swim_port:,
      region:,
      kv_port:,
      pubsub_port:,
    ))
  }
}

pub type NodeState {
  Alive
  Suspect
  Dead
}

pub fn encode_node_state(state: NodeState) -> json.Json {
  json.string(case state {
    Alive -> "alive"
    Suspect -> "suspect"
    Dead -> "dead"
  })
}

pub fn decode_node_state() -> decode.Decoder(NodeState) {
  {
    use text <- decode.then(decode.string)

    case text {
      "alive" -> decode.success(Alive)
      "suspect" -> decode.success(Suspect)
      "dead" -> decode.success(Dead)
      _ -> decode.failure(Alive, "NodeState")
    }
  }
}

pub opaque type Message {
  GetNode(id: String, recv: process.Subject(Result(NodeInfo, Nil)))
  ListNodes(recv: process.Subject(List(NodeInfo)))
  UpdateNode(node: NodeInfo)
}

type State {
  State(nodes: List(NodeInfo))
}

fn initialize(
  self: process.Subject(Message),
) -> Result(actor.Initialised(State, Message, SwimStore), String) {
  let return = SwimStore(self)

  let state = State(nodes: [])

  actor.initialised(state)
  |> actor.returning(return)
  |> Ok
}

fn on_message(state: State, message: Message) -> actor.Next(State, Message) {
  case message {
    GetNode(id:, recv:) -> handle_get_node(state, id, recv)
    ListNodes(recv:) -> handle_list_nodes(state, recv)
    UpdateNode(node:) -> handle_update_node(state, node)
  }
}

fn handle_get_node(
  state: State,
  id: String,
  recv: process.Subject(Result(NodeInfo, Nil)),
) -> actor.Next(State, Message) {
  list.find(state.nodes, fn(node) { node.id == id })
  |> process.send(recv, _)

  actor.continue(state)
}

fn handle_list_nodes(
  state: State,
  recv: process.Subject(List(NodeInfo)),
) -> actor.Next(State, Message) {
  process.send(recv, state.nodes)

  actor.continue(state)
}

fn handle_update_node(
  state: State,
  update: NodeInfo,
) -> actor.Next(State, Message) {
  let node_exists = list.any(state.nodes, fn(node) { node.id == update.id })

  let new_nodes = case node_exists {
    False -> [update, ..state.nodes]
    True ->
      list.map(state.nodes, fn(node) {
        use <- bool.guard(when: node.id != update.id, return: node)
        case update.version > node.version {
          True -> update
          False -> node
        }
      })
  }

  actor.continue(State(nodes: new_nodes))
}

pub fn new() -> Result(SwimStore, Nil) {
  let start_result =
    actor.new_with_initialiser(500, initialize)
    |> actor.on_message(on_message)
    |> actor.start

  case start_result {
    Error(error) -> {
      io.println_error("Error starting swim store")
      echo error
      Error(Nil)
    }
    Ok(start_result) -> {
      process.link(start_result.pid)
      Ok(start_result.data)
    }
  }
}

pub fn get_node(store: SwimStore, id: String) {
  process.call(store.subject, 1000, GetNode(id, _))
}

pub fn list_nodes(store: SwimStore) {
  process.call(store.subject, 1000, ListNodes)
}

pub fn update_node(store: SwimStore, node: NodeInfo) {
  process.send(store.subject, UpdateNode(node))
}
