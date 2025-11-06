import questly/swim

pub type ApiContext {
  Context(swim: swim.Swim, cluster_secret: String)
}
