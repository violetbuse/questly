import dot_env as dot
import gleam/erlang/process
import questly/program

pub fn main() -> Nil {
  dot.new()
  |> dot.set_path("./.env")
  |> dot.set_debug(False)
  |> dot.load

  let assert Ok(supervisor_start_result) =
    program.generate_config()
    |> program.start()
    as "program failed to start"

  process.link(supervisor_start_result.pid)

  process.sleep_forever()
}
