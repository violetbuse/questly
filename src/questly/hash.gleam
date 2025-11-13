import gleam/bit_array
import gleam/crypto
import gleam/int
import gleam/io
import gleam/list
import gleam/result
import questly/swim_store

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

pub fn is_current_node_replica(
  input: String,
  replicas: Int,
  self: swim_store.NodeInfo,
  remote: List(swim_store.NodeInfo),
) -> Bool {
  let input_hash = compute_hash(input)

  let sorted_nodes =
    list.sort([self, ..remote], fn(a, b) { int.compare(a.hash, b.hash) })
  let #(smaller, bigger) =
    list.split_while(sorted_nodes, fn(node) { node.hash < input_hash })

  let nodes = list.append(bigger, smaller)
  let replicas = list.take(nodes, replicas)

  list.contains(replicas, self)
}
