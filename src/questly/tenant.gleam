import questly/kv
import questly/pubsub
import questly/tenant_rate_limit_manager
import questly/tenant_rate_limiter

pub fn notify_tenant_created(pubsub: pubsub.Pubsub, _kv: kv.Kv, id: String) {
  tenant_rate_limit_manager.notify_new_tenant(pubsub, id)
}

pub fn notify_tenant_updated(pubsub: pubsub.Pubsub, kv: kv.Kv, id: String) {
  tenant_rate_limiter.new(id, pubsub, kv)
  |> tenant_rate_limiter.refresh()
}
