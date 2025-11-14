import gleam/dynamic/decode
import gleam/http
import gleam/json
import gleam/list
import gleam/result
import questly/api/api_utils
import questly/api/context
import questly/tenant
import questly/tenant/sql
import wisp

pub fn router(req: wisp.Request, context: context.ApiContext) -> wisp.Response {
  use <- api_utils.use_auth(req, context)

  case req.method, wisp.path_segments(req) {
    http.Get, [_, id] -> get_tenant(id, req, context)
    http.Post, [_] -> create_tenant(req, context)
    http.Patch, [_, id] -> update_tenant(id, req, context)
    _, _ -> wisp.not_found()
  }
}

fn get_tenant(
  id: String,
  _req: wisp.Request,
  context: context.ApiContext,
) -> wisp.Response {
  let tenant = {
    use rows <- result.try(
      sql.get_tenant(context.db, id)
      |> result.replace_error(wisp.internal_server_error()),
    )

    use row <- result.try(
      list.first(rows.rows)
      |> result.replace_error(wisp.not_found()),
    )

    Ok(row)
  }

  case tenant {
    Error(res) -> res
    Ok(tenant) -> {
      json.object([
        #("id", json.string(tenant.id)),
        #("created_at", json.int(tenant.created_at)),
        #("per_day_limit", json.int(tenant.per_day_limit)),
        #("remaining", json.int(tenant.tokens)),
      ])
      |> json.to_string
      |> wisp.json_response(200)
    }
  }
}

fn create_tenant(
  req: wisp.Request,
  context: context.ApiContext,
) -> wisp.Response {
  let create_decoder = {
    use id <- decode.field("id", decode.string)
    use per_day_limit <- decode.field("per_day_limit", decode.int)

    decode.success(#(id, per_day_limit))
  }

  use json <- wisp.require_json(req)

  let created = {
    use #(id, limit) <- result.try(
      decode.run(json, create_decoder)
      |> result.replace_error(wisp.bad_request("Invalid body")),
    )

    use _ <- result.try(
      sql.create_tenant(context.db, id, limit)
      |> result.replace_error(wisp.internal_server_error()),
    )

    tenant.notify_tenant_created(context.pubsub, context.kv, id)

    use rows <- result.try(
      sql.get_tenant(context.db, id)
      |> result.replace_error(wisp.internal_server_error()),
    )

    use row <- result.try(
      list.first(rows.rows)
      |> result.replace_error(wisp.internal_server_error()),
    )

    Ok(row)
  }

  case created {
    Error(res) -> res
    Ok(tenant) -> {
      json.object([
        #("id", json.string(tenant.id)),
        #("created_at", json.int(tenant.created_at)),
        #("per_day_limit", json.int(tenant.per_day_limit)),
        #("remaining", json.int(tenant.tokens)),
      ])
      |> json.to_string
      |> wisp.json_response(200)
    }
  }
}

fn update_tenant(
  id: String,
  req: wisp.Request,
  context: context.ApiContext,
) -> wisp.Response {
  let update_decoder = {
    use new_limit <- decode.field("per_day_limit", decode.int)

    decode.success(new_limit)
  }

  use json <- wisp.require_json(req)

  let updated = {
    use new_limit <- result.try(
      decode.run(json, update_decoder)
      |> result.replace_error(wisp.bad_request("Invalid body")),
    )

    use _ <- result.try(
      sql.set_limit(context.db, id, new_limit)
      |> result.replace_error(wisp.internal_server_error()),
    )

    tenant.notify_tenant_updated(context.pubsub, context.kv, id)

    use rows <- result.try(
      sql.get_tenant(context.db, id)
      |> result.replace_error(wisp.internal_server_error()),
    )

    use row <- result.try(
      list.first(rows.rows)
      |> result.replace_error(wisp.internal_server_error()),
    )

    Ok(row)
  }

  case updated {
    Error(res) -> res
    Ok(tenant) -> {
      json.object([
        #("id", json.string(tenant.id)),
        #("created_at", json.int(tenant.created_at)),
        #("per_day_limit", json.int(tenant.per_day_limit)),
        #("remaining", json.int(tenant.tokens)),
      ])
      |> json.to_string
      |> wisp.json_response(200)
    }
  }
}
