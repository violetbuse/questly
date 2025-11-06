import gleam/int

pub fn internal_url(hostname: String, port: Int, path: String) -> String {
  "http://" <> hostname <> ":" <> int.to_string(port) <> path
}
