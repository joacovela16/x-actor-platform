package xplat.flow.queue

import cats.effect.{Async, Resource}
import xplat.core.connectors.Publisher
import xplat.flow.nodes.{ClientPool, Connections}

/** The publishers, kept in the [[ClientPool]] of the server: one per resolved connection and destination, shared by every node and
  * instance.
  */
trait PublisherPool[F[_]] {

  /** Lends the publisher of this connection and destination while `f` runs. */
  def use[A](connection: Connections.Resolved, destination: String, destinationType: String)(f: Publisher[F, Array[Byte]] => F[A]): F[A]
}

object PublisherPool {

  def kind[F[_]](factory: PublisherFactory[F]): ClientPool.Kind[F, Publisher[F, Array[Byte]]] =
    new ClientPool.Kind[F, Publisher[F, Array[Byte]]] {
      val id = "publisher"
      def open(c: Connections.Resolved, extra: String): Resource[F, Publisher[F, Array[Byte]]] = {
        val (destinationType, destination) = extra.span(_ != ':')
        factory.open(c, destination.drop(1), destinationType)
      }
    }

  def on[F[_]](clients: ClientPool[F], factory: PublisherFactory[F]): PublisherPool[F] = {
    val k = kind[F](factory)
    new PublisherPool[F] {
      def use[A](c: Connections.Resolved, destination: String, destinationType: String)(f: Publisher[F, Array[Byte]] => F[A]): F[A] =
        clients.use(k, c, s"$destinationType:$destination")(f)
    }
  }

  /** A pool of its own (for tests and for a server without a shared one). */
  def make[F[_]](factory: PublisherFactory[F])(implicit A: Async[F]): Resource[F, PublisherPool[F]] = ClientPool.make[F]().map(on[F](_, factory))
}
