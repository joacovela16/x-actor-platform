package xplat.flow.mongo

import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.mongodb.client.{MongoClients, MongoDatabase}
import xplat.flow.nodes.{ClientPool, Connections}

/** The MongoDB clients, kept in the [[ClientPool]] of the server: one per resolved connection, shared by every node and instance. */
trait MongoPool[F[_]] {

  /** Lends the database of this connection, on its client, while `f` runs. */
  def use[A](connection: Connections.Resolved)(f: MongoDatabase => F[A]): F[A]
}

object MongoPool {

  def kind[F[_]](implicit A: Async[F]): ClientPool.Kind[F, MongoDatabase] =
    new ClientPool.Kind[F, MongoDatabase] {
      val id = "mongodb"
      def open(c: Connections.Resolved, extra: String): Resource[F, MongoDatabase] =
        Resource.eval(A.fromEither(MongoKinds.client(c).leftMap(new IllegalArgumentException(_)))).flatMap { settings =>
          // Creating a client does not connect: a server that is down is an error of the node that uses it.
          Resource.make(A.blocking(MongoClients.create(settings.settings)))(client => A.blocking(client.close()).attempt.void).map(_.getDatabase(settings.database))
        }
    }

  def on[F[_]: Async](clients: ClientPool[F]): MongoPool[F] = {
    val k = kind[F]
    new MongoPool[F] {
      def use[A](c: Connections.Resolved)(f: MongoDatabase => F[A]): F[A] = clients.use(k, c)(f)
    }
  }

  /** A pool of its own (for tests and for a server without a shared one). */
  def make[F[_]: Async]: Resource[F, MongoPool[F]] = ClientPool.make[F]().map(on[F])
}
