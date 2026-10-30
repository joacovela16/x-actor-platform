package xplat.core.cluster

import cats.effect.Resource
import xplat.core.api.ActorSystem
import xplat.core.cluster.api._

/** Membership of the local actor system in a cluster of nodes.
  *
  * {{{
  * ActorSystem[IO].flatMap(system => Cluster(system, ClusterConfig(seedNodes = seeds))).use { cluster =>
  *   cluster.events.collect { case ClusterEvent.MemberUp(m) => m }.evalMap(m => IO.println(s"up: $m")).compile.drain
  * }
  * }}}
  */
trait Cluster[F[_]] {

  /** The actor system this cluster node runs on. */
  def system: ActorSystem[F]

  def selfMember: F[Member]

  def state: F[ClusterState]

  /** Cluster changes. Each subscriber first receives the current state as events (e.g. a `MemberUp` per member that is
    * already up), then every change from that point on.
    */
  def events: fs2.Stream[F, ClusterEvent]

  /** Joins the cluster through the given nodes. Completes once this node is accepted (status Joining or later). */
  def join(seedNodes: List[Address]): F[Unit]

  /** Completes when this node reaches status Up. */
  def awaitUp: F[Unit]

  /** Asks a member to leave gracefully: it hands over its work and then is removed. */
  def leave(address: Address): F[Unit]

  /** Marks a member as down. Use it for crashed or permanently unreachable nodes; a downed member cannot come back. */
  def down(address: Address): F[Unit]

  def refResolver: RefResolver[F]

  /** Registry to find actors running on any node. */
  def receptionist: Receptionist[F]
}

object Cluster {

  /** Starts the cluster node. Releasing the resource leaves the cluster gracefully. */
  def apply[F[_]](system: ActorSystem[F], config: ClusterConfig = ClusterConfig())(implicit builder: ClusterBuilder[F]): Resource[F, Cluster[F]] =
    builder.build(system, config)
}
