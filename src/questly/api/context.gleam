import questly/kv
import questly/pubsub
import questly/swim

pub type ApiContext {
  Context(
    swim: swim.Swim,
    pubsub: pubsub.Pubsub,
    kv: kv.Kv,
    cluster_secret: String,
  )
}
