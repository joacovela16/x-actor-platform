package xplat.core.io.cluster

import cats.Eq
import cats.effect.kernel.{Async, Ref}
import cats.effect.std.Supervisor
import cats.implicits._
import fs2.concurrent.SignallingRef
import xplat.core.api.ActorRef
import xplat.core.api.error.XError
import xplat.core.cluster.Receptionist
import xplat.core.cluster.api._
import xplat.core.io.cluster.internal._

/** Receptionist whose registry is replicated to every member.
  *
  * Each node owns the entries of its own actors and sends them to the other members on every gossip round and on each
  * change. A registered actor gets an endpoint, so other nodes reach it through the remoting layer.
  */
final class IOReceptionist[F[_]] private (
    self: UniqueAddress,
    remoting: Remoting[F],
    asks: Asks[F],
    sendWire: (Address, Wire) => F[Unit],
    gossip: Ref[F, GossipState],
    local: Ref[F, Map[String, Set[String]]],
    localVersion: Ref[F, Long],
    remote: Ref[F, Map[UniqueAddress, (Long, Map[String, Set[String]])]],
    changes: SignallingRef[F, Long],
    supervisor: Supervisor[F]
)(implicit A: Async[F])
    extends Receptionist[F] {

  private def endpoint[T](key: ServiceKey[T], ref: ActorRef[F, T]): String = s"/receptionist/${key.id}/${ref.path}"

  private def changed: F[Unit] = changes.update(_ + 1)

  def register[T](key: ServiceKey[T], ref: ActorRef[F, T]): F[Unit] = {
    val path = endpoint(key, ref)
    val deliver: Array[Byte] => F[Unit] = bytes =>
      A.fromEither(key.codec.decode(bytes).leftMap(e => XError(e.message))).flatMap(ref.send)
    remoting.on(path)(deliver) >>
      local.update(m => m.updated(key.id, m.getOrElse(key.id, Set.empty) + path)) >>
      localVersion.update(_ + 1) >> changed >> broadcast >>
      supervisor.supervise(ref.terminated >> deregister(key, ref)).void
  }

  def deregister[T](key: ServiceKey[T], ref: ActorRef[F, T]): F[Unit] = {
    val path = endpoint(key, ref)
    remoting.off(path) >>
      local.update { m =>
        val rest = m.getOrElse(key.id, Set.empty) - path
        if (rest.isEmpty) m - key.id else m.updated(key.id, rest)
      } >> localVersion.update(_ + 1) >> changed >> broadcast
  }

  def find[T](key: ServiceKey[T]): F[Listing[F, T]] =
    (gossip.get, local.get, remote.get).mapN { (g, mine, others) =>
      def live(node: UniqueAddress): Boolean =
        g.members.get(node).exists(m => m.status != MemberStatus.Down) && !g.unreachable(node)

      val localRefs = mine.getOrElse(key.id, Set.empty).map(p => EndpointRef[F, T](self.address, p)(remoting, asks, key.codec): ClusterRef[F, T])
      val remoteRefs = others.toSet[(UniqueAddress, (Long, Map[String, Set[String]]))].collect {
        case (node, (_, entries)) if live(node) => entries.getOrElse(key.id, Set.empty).map(p => EndpointRef[F, T](node.address, p)(remoting, asks, key.codec): ClusterRef[F, T])
      }.flatten
      Listing(key, localRefs ++ remoteRefs)
    }

  def subscribe[T](key: ServiceKey[T]): fs2.Stream[F, Listing[F, T]] =
    changes.discrete.evalMap(_ => find(key)).changes(Eq.fromUniversalEquals[Listing[F, T]])

  /** Sends the local entries to every other member. */
  def broadcast: F[Unit] =
    for {
      g       <- gossip.get
      version <- localVersion.get
      entries <- local.get
      message = Wire.Registry(self, version, entries)
      _ <- g.members.keys.filter(_ != self).toList.traverse_(node => sendWire(node.address, message))
    } yield ()

  def receive(registry: Wire.Registry): F[Unit] =
    remote.modify { current =>
      if (current.get(registry.from).exists(_._1 >= registry.version)) (current, false)
      else (current.updated(registry.from, (registry.version, registry.entries)), true)
    }.ifM(changed, A.unit)

  /** Drops the entries of removed members and refreshes subscribers. */
  def membershipChanged(g: GossipState): F[Unit] =
    remote.update(_.filter { case (node, _) => g.members.contains(node) }) >> changed
}

object IOReceptionist {
  def apply[F[_]](
      self: UniqueAddress,
      remoting: Remoting[F],
      asks: Asks[F],
      sendWire: (Address, Wire) => F[Unit],
      gossip: Ref[F, GossipState],
      supervisor: Supervisor[F]
  )(implicit A: Async[F]): F[IOReceptionist[F]] =
    (
      Ref.of[F, Map[String, Set[String]]](Map.empty),
      Ref.of[F, Long](0L),
      Ref.of[F, Map[UniqueAddress, (Long, Map[String, Set[String]])]](Map.empty),
      SignallingRef.of[F, Long](0L)
    ).mapN(new IOReceptionist[F](self, remoting, asks, sendWire, gossip, _, _, _, _, supervisor))
}
