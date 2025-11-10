import gleam/dynamic/decode
import gleam/erlang/process
import gleam/http/request
import gleam/int
import gleam/io
import gleam/json
import gleam/otp/actor
import gleam/result
import httpp/send
import mist
import questly/swim_store.{type NodeInfo}
import wisp
import wisp/wisp_mist

pub opaque type Channel(message, response) {
  Channel(subject: process.Subject(Message(message, response)))
}

pub opaque type Builder(message, response, client) {
  Builder(
    port: Result(Int, String),
    get_port: Result(fn(NodeInfo) -> Int, String),
    encode_message: Result(fn(message) -> json.Json, String),
    encode_response: Result(fn(response) -> json.Json, String),
    decode_message: Result(decode.Decoder(message), String),
    decode_response: Result(decode.Decoder(response), String),
    message_mapper: Result(
      fn(message, process.Subject(response)) -> client,
      String,
    ),
    response_mapper: Result(fn(response) -> client, String),
    sender: Result(process.Subject(client), String),
    cluster_secret: Result(String, String),
  )
}

pub fn new() -> Builder(message, response, client) {
  Builder(
    port: Error("Please provide a valid port to channel builder."),
    get_port: Error(
      "Please provide a valid get_port function to channel builder.",
    ),
    encode_message: Error(
      "Please provide a valid encode_message function to channel builder.",
    ),
    encode_response: Error(
      "Please provide a valid encode_response function to channel builder.",
    ),
    decode_message: Error(
      "Please provide a valid decode_message function to channel builder.",
    ),
    decode_response: Error(
      "Please provide a valid decode_response function to channel builder.",
    ),
    message_mapper: Error(
      "Please provide a valid message_mapper function to channel builder.",
    ),
    response_mapper: Error(
      "Please provide a valid response_mapper function to channel builder.",
    ),
    sender: Error("Please provide a valid sender subject to channel builder."),
    cluster_secret: Error(
      "Please provide a valid cluster secret to channel builder.",
    ),
  )
}

pub fn port(
  builder: Builder(message, response, client),
  port: Int,
) -> Builder(message, response, client) {
  Builder(..builder, port: Ok(port))
}

pub fn get_port(
  builder: Builder(message, response, client),
  get_port: fn(NodeInfo) -> Int,
) -> Builder(message, response, client) {
  Builder(..builder, get_port: Ok(get_port))
}

pub fn encode_message(
  builder: Builder(message, response, client),
  encode_message: fn(message) -> json.Json,
) -> Builder(message, response, client) {
  Builder(..builder, encode_message: Ok(encode_message))
}

pub fn encode_response(
  builder: Builder(message, response, client),
  encode_response: fn(response) -> json.Json,
) -> Builder(message, response, client) {
  Builder(..builder, encode_response: Ok(encode_response))
}

pub fn decode_message(
  builder: Builder(message, response, client),
  decode_message: decode.Decoder(message),
) -> Builder(message, response, client) {
  Builder(..builder, decode_message: Ok(decode_message))
}

pub fn decode_response(
  builder: Builder(message, response, client),
  decode_response: decode.Decoder(response),
) -> Builder(message, response, client) {
  Builder(..builder, decode_response: Ok(decode_response))
}

pub fn message_mapper(
  builder: Builder(message, response, client),
  message_mapper: fn(message, process.Subject(response)) -> client,
) -> Builder(message, response, client) {
  Builder(..builder, message_mapper: Ok(message_mapper))
}

pub fn response_mapper(
  builder: Builder(message, response, client),
  response_mapper: fn(response) -> client,
) -> Builder(message, response, client) {
  Builder(..builder, response_mapper: Ok(response_mapper))
}

pub fn sender(
  builder: Builder(message, response, client),
  sender: process.Subject(client),
) -> Builder(message, response, client) {
  Builder(..builder, sender: Ok(sender))
}

pub fn cluster_secret(
  builder: Builder(message, response, client),
  cluster_secret: String,
) -> Builder(message, response, client) {
  Builder(..builder, cluster_secret: Ok(cluster_secret))
}

pub opaque type Message(message, response) {
  SendMessage(node: NodeInfo, message: message)
  SendBlocking(
    node: NodeInfo,
    message: message,
    recv: process.Subject(response),
  )
  HandleRequest(body: String, recv: process.Subject(json.Json))
}

type State(message, response, client) {
  State(
    port: Int,
    get_port: fn(NodeInfo) -> Int,
    encode_message: fn(message) -> json.Json,
    decode_message: decode.Decoder(message),
    encode_response: fn(response) -> json.Json,
    decode_response: decode.Decoder(response),
    message_mapper: fn(message, process.Subject(response)) -> client,
    response_mapper: fn(response) -> client,
    sender: process.Subject(client),
    cluster_secret: String,
  )
}

fn initialize(
  self: process.Subject(Message(message, response)),
  builder: Builder(message, response, client),
) -> Result(
  actor.Initialised(
    State(message, response, client),
    Message(message, response),
    Channel(message, response),
  ),
  String,
) {
  let return = Channel(subject: self)

  use port <- result.try(builder.port)
  use get_port <- result.try(builder.get_port)
  use encode_message <- result.try(builder.encode_message)
  use decode_message <- result.try(builder.decode_message)
  use encode_response <- result.try(builder.encode_response)
  use decode_response <- result.try(builder.decode_response)
  use message_mapper <- result.try(builder.message_mapper)
  use response_mapper <- result.try(builder.response_mapper)
  use sender <- result.try(builder.sender)
  use cluster_secret <- result.try(builder.cluster_secret)

  let state =
    State(
      port: port,
      get_port: get_port,
      encode_message: encode_message,
      decode_message: decode_message,
      encode_response: encode_response,
      decode_response: decode_response,
      message_mapper: message_mapper,
      response_mapper: response_mapper,
      sender: sender,
      cluster_secret: cluster_secret,
    )

  let api_started =
    wisp_mist.handler(on_request(_, self), cluster_secret)
    |> mist.new
    |> mist.bind("0.0.0.0")
    |> mist.with_ipv6
    |> mist.port(port)
    |> mist.start

  use api_start_result <- result.try(
    api_started |> result.replace_error("Error starting channel api."),
  )

  process.link(api_start_result.pid)

  actor.initialised(state)
  |> actor.returning(return)
  |> Ok
}

fn on_request(
  req: wisp.Request,
  self: process.Subject(Message(message, response)),
) -> wisp.Response {
  use body <- wisp.require_string_body(req)

  process.call_forever(self, HandleRequest(body, _))
  |> json.to_string
  |> wisp.json_response(200)
}

fn send_request(
  node: NodeInfo,
  message: message,
  state: State(message, response, client),
) -> Result(response, String) {
  let body = state.encode_message(message) |> json.to_string
  let port = state.get_port(node)

  let url = "http://" <> node.hostname <> ":" <> int.to_string(port) <> "/"

  let assert Ok(base_req) = request.to(url)

  let req =
    base_req
    |> request.set_query([#("secret", state.cluster_secret)])
    |> request.set_body(body)

  let result =
    send.send(req)
    |> result.map_error(fn(error) {
      io.println_error("Error sending request to channel")
      io.println_error(body)
      echo error
      "Error sending request to channel"
    })

  use res <- result.try(result)

  let response =
    json.parse(res.body, state.decode_response)
    |> result.map_error(fn(error) {
      io.println_error("Error parsing response from channel")
      io.println_error(body)
      echo error
      "Error parsing response from channel"
    })

  use response_data <- result.try(response)

  Ok(response_data)
}

fn on_message(
  state: State(message, response, client),
  message: Message(message, response),
) -> actor.Next(State(message, response, client), Message(message, response)) {
  process.spawn(fn() {
    case message {
      HandleRequest(body:, recv:) -> {
        let response_channel = process.new_subject()

        let client_message_result =
          json.parse(body, state.decode_message)
          |> result.map(fn(message) {
            state.message_mapper(message, response_channel)
          })
          |> result.map_error(fn(error) {
            io.println_error("Error parsing message from channel")
            io.println_error(body)
            echo error
            "Error parsing message from channel"
          })

        use client_message <- result.try(client_message_result)
        process.send(state.sender, client_message)
        let result = process.receive_forever(response_channel)

        state.encode_response(result)
        |> process.send(recv, _)

        Ok(Nil)
      }
      SendBlocking(node:, message:, recv:) -> {
        use response <- result.try(send_request(node, message, state))

        process.send(recv, response)

        Ok(Nil)
      }
      SendMessage(node:, message:) -> {
        use response <- result.try(send_request(node, message, state))
        let client = state.response_mapper(response)
        process.send(state.sender, client)

        Ok(Nil)
      }
    }
  })

  actor.continue(state)
}

pub fn start(builder: Builder(a, b, c)) {
  let start_result =
    actor.new_with_initialiser(1000, initialize(_, builder))
    |> actor.on_message(on_message)
    |> actor.start()

  case start_result {
    Error(error) -> {
      io.println("error starting channel")
      echo error
      Error(error)
    }
    Ok(start_result) -> {
      process.link(start_result.pid)
      Ok(start_result.data)
    }
  }
}
