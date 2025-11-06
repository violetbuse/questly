import gleam/bit_array
import gleam/crypto
import gleam/int
import gleam/io
import gleam/result

pub fn compute_hash(input: String) -> Int {
  bit_array.from_string(input)
  |> crypto.hash(crypto.Sha256, _)
  |> bit_array.slice(at: 0, take: 4)
  |> result.map(bit_array.base16_encode)
  |> result.map(int.base_parse(_, 16))
  |> result.flatten
  |> result.lazy_unwrap(fn() {
    io.println_error("Failed to compute hash for " <> input)
    panic as "Failed to compute hash"
  })
}
