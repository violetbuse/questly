import gleam/bool
import gleam/dynamic/decode
import gleam/erlang/process
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
import gleam/string
import httpp/send
import mist
import questly/hash
import questly/swim_store.{type NodeInfo, Alive, Dead, NodeInfo, Suspect}
import questly/util
import wisp
import wisp/wisp_mist

pub opaque type Swim {
  Swim(subject: process.Subject(Message))
}

pub fn from_name(name: process.Name(Message)) {
  Swim(process.named_subject(name))
}

pub type SwimConfig {
  SwimConfig(
    name: process.Name(Message),
    node_id: String,
    hostname: String,
    region: String,
    bootstrap_hosts: List(String),
    port: Int,
    secret: String,
    kv_port: Int,
    pubsub_port: Int,
  )
}

pub opaque type Message {
  Heartbeat
  HealthCheck(recv: process.Subject(Nil))
  LinkApi(pid: process.Pid)
  IncomingSync(req: SyncRequest, recv: process.Subject(SyncResponse))
  IncomingPing(recv: process.Subject(Nil))
  NodeUpdate(node: NodeInfo)
  YouUpdate(node: NodeInfo)
  GetSelf(recv: process.Subject(NodeInfo))
  GetNodes(recv: process.Subject(List(NodeInfo)))
}

type State {
  State(
    self: NodeInfo,
    store: swim_store.SwimStore,
    subject: process.Subject(Message),
    bootstrap_hosts: List(String),
    port: Int,
    secret: String,
  )
}

fn initialize(
  self: process.Subject(Message),
  config: SwimConfig,
) -> Result(actor.Initialised(State, Message, Swim), String) {
  let result = Swim(self)

  let assert Ok(swim_store) = swim_store.new()
    as "swim could not start swim store"

  let state =
    State(
      self: NodeInfo(
        id: config.node_id,
        hash: hash.compute_hash(config.node_id),
        version: 1,
        state: Alive,
        hostname: config.hostname,
        swim_port: config.port,
        region: config.region,
        kv_port: config.kv_port,
        pubsub_port: config.pubsub_port,
      ),
      store: swim_store,
      subject: self,
      bootstrap_hosts: config.bootstrap_hosts,
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
    Heartbeat -> handle_heartbeat(state)
    LinkApi(pid:) -> handle_link_api(state, pid)
    IncomingPing(recv:) -> handle_incoming_ping(state, recv)
    IncomingSync(req:, recv:) -> handle_incoming_sync(state, req, recv)
    NodeUpdate(node:) -> handle_node_update(state, node)
    YouUpdate(node:) -> handle_you_update(state, node)
    GetNodes(recv:) -> handle_get_nodes(state, recv)
    GetSelf(recv:) -> handle_get_self(state, recv)
    HealthCheck(recv:) -> handle_health_check(state, recv)
  }
}

const heartbeat_interval = 5000

fn handle_heartbeat(state: State) -> actor.Next(State, Message) {
  process.send_after(state.subject, heartbeat_interval, Heartbeat)

  process.spawn(fn() {
    let nodelist = swim_store.list_nodes(state.store)

    let alive_candidates =
      list.filter(nodelist, swim_store.is_alive)
      |> list.sample(3)
      |> list.map(fn(node) {
        #(node.hostname, node.swim_port, option.Some(node))
      })

    let suspect_candidates =
      list.filter(nodelist, swim_store.is_suspect)
      |> list.sample(int.random(1))
      |> list.map(fn(node) {
        #(node.hostname, node.swim_port, option.Some(node))
      })

    let dead_candidates =
      list.filter(nodelist, swim_store.is_dead)
      |> list.sample(int.random(1))
      |> list.map(fn(node) { #(node.hostname, node.swim_port, option.None) })

    let bootstrap_candidates =
      state.bootstrap_hosts
      |> list.filter(fn(host) {
        list.any(nodelist, fn(node) { node.hostname == host }) |> bool.negate
      })
      |> list.sample(2)
      |> list.map(fn(host) {
        case string.split_once(host, ":") {
          Ok(#(hostname, port)) ->
            case int.parse(port) {
              Ok(port) -> #(hostname, port, option.None)
              Error(_) -> #(hostname, state.port, option.None)
            }
          Error(_) -> #(host, state.port, option.None)
        }
      })

    let candidates =
      []
      |> list.append(alive_candidates)
      |> list.append(suspect_candidates)
      |> list.append(dead_candidates)
      |> list.append(bootstrap_candidates)

    sync_candidates(state, candidates)
  })

  actor.continue(state)
}

fn sync_candidates(
  state: State,
  candidates: List(#(String, Int, option.Option(NodeInfo))),
) {
  let nodelist = swim_store.list_nodes(state.store)

  list.each(candidates, fn(node) {
    process.spawn(fn() {
      let #(host, port, you) = node
      let sync_result =
        send_sync_request(
          host:,
          port:,
          secret: state.secret,
          send: SyncRequest(
            self: state.self,
            you:,
            nodes: list.sample(nodelist, 3),
          ),
        )

      case sync_result {
        Error(_) -> {
          let node_id = list.find(nodelist, fn(node) { node.hostname == host })

          use <- bool.guard(when: result.is_error(node_id), return: Nil)
          let assert Ok(NodeInfo(id: node_id, ..)) = node_id

          let _ =
            process.spawn_unlinked(fn() {
              let assert Ok(node) = swim_store.get_node(state.store, node_id)

              use <- bool.guard(when: node.state == Dead, return: Nil)

              let #(next_state, next_version) = case node.state {
                Alive -> #(Suspect, node.version + 1)
                _ -> #(node.state, node.version)
              }

              let interim_node =
                NodeInfo(..node, state: next_state, version: next_version)

              swim_store.update_node(state.store, interim_node)

              let alive_nodes =
                swim_store.list_nodes(state.store)
                |> list.filter(fn(node) { node.state == Alive })
                |> list.shuffle
                |> list.first

              let alive_node = result.unwrap(alive_nodes, state.self)

              let verification_result =
                send_checkup_request(
                  host: alive_node.hostname,
                  port: state.port,
                  secret: state.secret,
                  node: interim_node,
                )
                |> result.map(fn(response) { response.success })
                |> result.unwrap(False)

              let new_state = case verification_result {
                True -> Alive
                False -> Dead
              }

              let new_node =
                NodeInfo(
                  ..interim_node,
                  state: new_state,
                  version: interim_node.version + 1,
                )

              swim_store.update_node(state.store, new_node)
            })

          Nil
        }
        Ok(SyncResponse(self:, you:, nodes:)) -> {
          process.send(state.subject, NodeUpdate(self))
          process.send(state.subject, YouUpdate(you))
          list.each(nodes, fn(node) {
            process.send(state.subject, NodeUpdate(node))
          })
        }
      }
    })
  })
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

fn handle_incoming_ping(
  state: State,
  recv: process.Subject(Nil),
) -> actor.Next(State, Message) {
  process.send(recv, Nil)
  actor.continue(state)
}

fn handle_incoming_sync(
  state: State,
  req: SyncRequest,
  recv: process.Subject(SyncResponse),
) -> actor.Next(State, Message) {
  let SyncRequest(self: req_self, you: req_you, nodes: req_nodes) = req

  process.send(state.subject, NodeUpdate(req_self))
  option.map(req_you, fn(node) { process.send(state.subject, YouUpdate(node)) })
  list.each(req_nodes, fn(node) {
    process.send(state.subject, NodeUpdate(node))
  })

  let nodelist = swim_store.list_nodes(state.store)

  let self = state.self
  let you =
    list.find(nodelist, fn(node) { node.id == req_self.id })
    |> result.unwrap(req_self)
  let nodes = list.sample(nodelist, 2)

  process.send(recv, SyncResponse(self:, you:, nodes:))

  actor.continue(state)
}

fn handle_node_update(
  state: State,
  node: NodeInfo,
) -> actor.Next(State, Message) {
  use <- bool.guard(
    when: node.id == state.self.id,
    return: actor.continue(state),
  )

  swim_store.update_node(state.store, node)

  actor.continue(state)
}

fn handle_you_update(
  state: State,
  remote: NodeInfo,
) -> actor.Next(State, Message) {
  let self = state.self

  let new_version = {
    use <- bool.guard(when: self.version > remote.version, return: self.version)
    use <- bool.guard(when: self == remote, return: self.version)
    remote.version + 1
  }

  let new_self = NodeInfo(..self, version: new_version)

  actor.continue(State(..state, self: new_self))
}

fn handle_get_self(
  state: State,
  recv: process.Subject(NodeInfo),
) -> actor.Next(State, Message) {
  process.send(recv, state.self)
  actor.continue(state)
}

fn handle_get_nodes(
  state: State,
  recv: process.Subject(List(NodeInfo)),
) -> actor.Next(State, Message) {
  process.send(recv, swim_store.list_nodes(state.store))
  actor.continue(state)
}

fn start_swim_actor(
  config: SwimConfig,
) -> Result(actor.Started(Swim), actor.StartError) {
  let start_result =
    actor.new_with_initialiser(1000, initialize(_, config))
    |> actor.on_message(on_message)
    |> actor.named(config.name)
    |> actor.start()

  case start_result {
    Error(error) -> {
      io.println_error("Error starting swim actor")
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
  Context(swim: Swim, port: Int, secret: String)
}

fn router(req: wisp.Request, context: Context) -> wisp.Response {
  case wisp.path_segments(req) {
    ["health"] -> handle_api_health_check(context)
    ["sync"] -> handle_sync_request(req, context)
    ["ping"] -> handle_ping_request(req, context)
    ["checkup"] -> handle_checkup_request(req, context)
    _ -> wisp.not_found()
  }
}

fn handle_api_health_check(context: Context) {
  process.call(context.swim.subject, 1000, HealthCheck)
  wisp.ok()
}

type SyncRequest {
  SyncRequest(
    self: NodeInfo,
    you: option.Option(NodeInfo),
    nodes: List(NodeInfo),
  )
}

fn encode_sync_request(req: SyncRequest) -> json.Json {
  json.object([
    #("self", swim_store.encode_node_info(req.self)),
    #("you", json.nullable(req.you, swim_store.encode_node_info)),
    #("nodes", json.array(req.nodes, swim_store.encode_node_info)),
  ])
}

fn decode_sync_request() -> decode.Decoder(SyncRequest) {
  {
    use self <- decode.field("self", swim_store.decode_node_info())
    use you <- decode.field(
      "you",
      decode.optional(swim_store.decode_node_info()),
    )
    use nodes <- decode.field(
      "nodes",
      decode.list(swim_store.decode_node_info()),
    )

    decode.success(SyncRequest(self:, you:, nodes:))
  }
}

type SyncResponse {
  SyncResponse(self: NodeInfo, you: NodeInfo, nodes: List(NodeInfo))
}

fn encode_sync_response(resp: SyncResponse) -> json.Json {
  json.object([
    #("self", swim_store.encode_node_info(resp.self)),
    #("you", swim_store.encode_node_info(resp.you)),
    #("nodes", json.array(resp.nodes, swim_store.encode_node_info)),
  ])
}

fn decode_sync_response() -> decode.Decoder(SyncResponse) {
  {
    use self <- decode.field("self", swim_store.decode_node_info())
    use you <- decode.field("you", swim_store.decode_node_info())
    use nodes <- decode.field(
      "nodes",
      decode.list(swim_store.decode_node_info()),
    )

    decode.success(SyncResponse(self:, you:, nodes:))
  }
}

fn handle_sync_request(req: wisp.Request, context: Context) -> wisp.Response {
  use <- wisp.require_method(req, http.Post)
  use <- wisp.require_content_type(req, "application/json")
  use json <- wisp.require_json(req)
  let decode = decode.run(json, decode_sync_request())

  case decode {
    Error(_) ->
      json.object([#("error", json.string("Malformed request"))])
      |> json.to_string
      |> wisp.json_response(400)
    Ok(request) -> {
      process.call(context.swim.subject, 1000, IncomingSync(request, _))
      |> encode_sync_response
      |> json.to_string
      |> wisp.json_response(200)
    }
  }
}

fn send_sync_request(
  host hostname: String,
  port port: Int,
  secret key: String,
  send request: SyncRequest,
) -> Result(SyncResponse, Nil) {
  let body = encode_sync_request(request) |> json.to_string

  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, "/sync"))
    |> result.map(request.set_method(_, http.Post))
    |> result.map(request.set_header(_, "content-type", "application/json"))
    |> result.map(request.set_query(_, [#("secret", key)]))
    |> result.map(request.set_body(_, body))
    as "could not create sync request"

  let server_response =
    send.send(request)
    |> result.map_error(fn(err) {
      io.println_error("error sending sync request to " <> hostname)
      echo err
    })
    |> result.replace_error(Nil)
  use response <- result.try(server_response)

  json.parse(response.body, decode_sync_response())
  |> result.replace_error(Nil)
}

fn handle_ping_request(_req: wisp.Request, context: Context) -> wisp.Response {
  let receiving_channel = process.new_subject()
  process.send(context.swim.subject, IncomingPing(receiving_channel))
  let received = process.receive(receiving_channel, 500) |> result.is_ok

  case received {
    False -> wisp.bad_request("")
    True -> wisp.ok()
  }
}

fn send_ping_request(
  host hostname: String,
  port port: Int,
  secret key: String,
  wait timeout: Int,
) -> Bool {
  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, "/ping"))
    |> result.map(request.set_query(_, [#("secret", key)]))
    as "could not create ping request"

  let response_receiver = process.new_subject()

  process.spawn_unlinked(fn() {
    let response = send.send(request)

    let success = case response {
      Error(err) -> {
        io.print_error(
          "Error pinging " <> hostname <> ":" <> int.to_string(port),
        )
        echo err
        False
      }
      Ok(response) -> response.status == 200
    }

    process.send(response_receiver, success)
  })

  process.receive(response_receiver, timeout) |> result.unwrap(False)
}

type CheckupRequest {
  CheckupRequest(node: NodeInfo)
}

fn encode_checkup_request(req: CheckupRequest) -> json.Json {
  json.object([#("node", swim_store.encode_node_info(req.node))])
}

fn decode_checkup_request() -> decode.Decoder(CheckupRequest) {
  {
    use node <- decode.field("node", swim_store.decode_node_info())
    decode.success(CheckupRequest(node:))
  }
}

type CheckupResponse {
  CheckupResponse(success: Bool)
}

fn encode_checkup_response(resp: CheckupResponse) -> json.Json {
  json.object([#("success", json.bool(resp.success))])
}

fn decode_checkup_response() -> decode.Decoder(CheckupResponse) {
  {
    use success <- decode.field("success", decode.bool)
    decode.success(CheckupResponse(success: success))
  }
}

fn handle_checkup_request(req: wisp.Request, context: Context) -> wisp.Response {
  use <- wisp.require_method(req, http.Post)
  use <- wisp.require_content_type(req, "application/json")
  use json <- wisp.require_json(req)
  let decode = decode.run(json, decode_checkup_request())

  case decode {
    Ok(request) -> {
      let ping_response =
        send_ping_request(
          host: request.node.hostname,
          port: context.port,
          secret: context.secret,
          wait: 2000,
        )

      CheckupResponse(ping_response)
      |> encode_checkup_response
      |> json.to_string
      |> wisp.json_response(200)
    }
    Error(_) ->
      json.object([#("error", json.string("Malformed request"))])
      |> json.to_string
      |> wisp.json_response(400)
  }
}

fn send_checkup_request(
  host hostname: String,
  port port: Int,
  secret key: String,
  node info: NodeInfo,
) -> Result(CheckupResponse, Nil) {
  let body = CheckupRequest(info) |> encode_checkup_request |> json.to_string

  let assert Ok(request) =
    request.to(util.internal_url(hostname, port, "/checkup"))
    |> result.map(request.set_method(_, http.Post))
    |> result.map(request.set_header(_, "content-type", "application/json"))
    |> result.map(request.set_query(_, [#("secret", key)]))
    |> result.map(request.set_body(_, body))
    as "could not create checkup request"

  let server_response =
    send.send(request)
    |> result.map_error(fn(err) {
      io.println_error(
        "Failed to send checkup request: "
        <> hostname
        <> ":"
        <> int.to_string(port),
      )
      echo err
    })
    |> result.replace_error(Nil)
  use response <- result.try(server_response)

  json.parse(response.body, decode_checkup_response())
  |> result.replace_error(Nil)
}

fn start_swim_api(swim: Swim, config: SwimConfig) {
  wisp.configure_logger()

  let context = Context(swim:, port: config.port, secret: config.secret)
  let start_result =
    wisp_mist.handler(router(_, context), config.secret)
    |> mist.new
    |> mist.bind("0.0.0.0")
    |> mist.with_ipv6
    |> mist.port(config.port)
    |> mist.start

  start_result
  |> result.replace_error(actor.InitFailed("failed to start swim api"))
}

pub fn supervised(config: SwimConfig) {
  supervision.worker(fn() {
    use actor.Started(_, swim) as start_result <- result.try(start_swim_actor(
      config,
    ))
    use actor.Started(api_pid, _) <- result.try(start_swim_api(swim, config))

    process.send(swim.subject, LinkApi(api_pid))

    Ok(start_result)
  })
}

pub fn get_self(swim: Swim) {
  process.call(swim.subject, 1000, GetSelf)
}

pub fn get_remote(swim: Swim) {
  process.call(swim.subject, 1000, GetNodes)
}
