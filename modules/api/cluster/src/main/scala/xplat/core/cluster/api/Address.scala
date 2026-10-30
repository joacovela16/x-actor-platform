package xplat.core.cluster.api

/** Network location of an actor system that belongs to a cluster. */
final case class Address(system: String, host: String, port: Int) {
  override def toString: String = s"xplat://$system@$host:$port"
}

object Address {

  /** Fixed order every node agrees on, used to break ties (e.g. which side survives an even split). */
  implicit val ordering: Ordering[Address] = Ordering.by(a => (a.system, a.host, a.port))
}

/** An [[Address]] plus the incarnation of the node, so a node restarted on the same host and port is a different member. */
final case class UniqueAddress(address: Address, uid: Long)

object UniqueAddress {
  implicit val ordering: Ordering[UniqueAddress] = Ordering.by(a => (a.address, a.uid))
}
