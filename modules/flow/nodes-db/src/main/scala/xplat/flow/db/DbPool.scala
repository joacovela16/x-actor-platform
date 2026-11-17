package xplat.flow.db

import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.zaxxer.hikari.HikariConfig
import doobie.Transactor
import doobie.hikari.HikariTransactor
import xplat.flow.nodes.{ClientPool, Connections}

/** The JDBC connection pools, kept in the [[ClientPool]] of the server: one per resolved connection, shared by every node and
  * instance. Each is a Hikari pool with its own connections (`maxPoolSize`).
  */
trait DbPool[F[_]] {

  /** Lends the pool of this connection, with the kind of database it is, while `f` runs. */
  def use[A](connection: Connections.Resolved)(f: DbPool.Open[F] => F[A]): F[A]
}

object DbPool {

  final case class Open[F[_]](kind: String, transactor: Transactor[F])

  def kind[F[_]](implicit A: Async[F]): ClientPool.Kind[F, Open[F]] =
    new ClientPool.Kind[F, Open[F]] {
      val id = "jdbc"
      def open(c: Connections.Resolved, extra: String): Resource[F, Open[F]] =
        Resource.eval(A.fromEither(DbKinds.jdbc(c).leftMap(new IllegalArgumentException(_)))).flatMap { settings =>
          HikariTransactor.fromHikariConfig[F](hikari(c, settings)).map(Open[F](settings.kind, _))
        }
    }

  def on[F[_]: Async](clients: ClientPool[F]): DbPool[F] = {
    val k = kind[F]
    new DbPool[F] {
      def use[A](c: Connections.Resolved)(f: Open[F] => F[A]): F[A] = clients.use(k, c)(f)
    }
  }

  /** A pool of its own (for tests and for a server without a shared one). */
  def make[F[_]: Async]: Resource[F, DbPool[F]] = ClientPool.make[F]().map(on[F])

  private def hikari(c: Connections.Resolved, s: DbKinds.Jdbc): HikariConfig = {
    val config = new HikariConfig()
    config.setDriverClassName(s.driver)
    config.setJdbcUrl(s.url)
    s.user.foreach(config.setUsername)
    s.password.foreach(config.setPassword)
    config.setMaximumPoolSize(s.maxPoolSize)
    config.setPoolName(s"flow-db-${c.name}")
    config.setConnectionTimeout(5000)
    // Do not connect while the pool is built: a database that is down shows up as an error of the node that uses it.
    config.setInitializationFailTimeout(-1)
    config
  }
}
