import pog
import questly/kv
import questly/pubsub
import questly/swim

pub type ApiContext {
  Context(
    swim: swim.Swim,
    pubsub: pubsub.Pubsub,
    kv: kv.Kv,
    db: pog.Connection,
    cluster_secret: String,
  )
}
