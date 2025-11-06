import dot_env as dot
import gleam/erlang/process
import questly/program

pub fn main() -> Nil {
  dot.new()
  |> dot.set_path("./.env")
  |> dot.set_debug(False)
  |> dot.load

  let assert Ok(_) =
    program.generate_config()
    |> program.start()
    as "program failed to start"

  process.sleep_forever()
}
