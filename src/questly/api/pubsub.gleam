import gleam/bool
import gleam/dict
import gleam/float
import gleam/int
import gleam/json
import gleam/result
import gleam/time/duration
import gleam/time/timestamp
import questly/api/api_utils
import questly/api/context
import questly/pubsub
import questly/pubsub_store
import wisp

pub fn router(req: wisp.Request, context: context.ApiContext) -> wisp.Response {
  use <- api_utils.use_auth(req, context)

  case wisp.path_segments(req) {
    [_, channel_name, "publish", id] ->
      handle_publish(channel_name, id, req, context)
    [_, channel_name, "replay", from] ->
      handle_replay_events(channel_name, from, context)
    _ -> wisp.not_found()
  }
}

fn handle_publish(
  channel_name: String,
  id: String,
  req: wisp.Request,
  context: context.ApiContext,
) -> wisp.Response {
  use data <- wisp.require_string_body(req)

  let one_month_from_now =
    timestamp.system_time()
    |> timestamp.add(duration.minutes(31 * 24 * 60))
    |> timestamp.to_unix_seconds
    |> float.round

  pubsub.publish(
    context.pubsub,
    channel_name,
    id,
    data,
    dict.new(),
    one_month_from_now,
  )
  |> pubsub_store.encode_event
  |> json.to_string
  |> wisp.json_response(200)
}

fn handle_replay_events(
  channel_name: String,
  from: String,
  context: context.ApiContext,
) -> wisp.Response {
  let malformed = wisp.response(400)
  let from_parsed = int.parse(from)
  use <- bool.guard(when: result.is_error(from_parsed), return: malformed)
  let assert Ok(from) = from_parsed

  let filter = fn(event: pubsub_store.Event) { event.channel == channel_name }

  pubsub.replay_events(context.pubsub, from, filter)
  |> json.array(pubsub_store.encode_event)
  |> json.to_string
  |> wisp.json_response(200)
}
