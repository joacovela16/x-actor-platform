package xplat.core.io.actor

import cats.effect.Sync
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.console.ConsoleLogger

/** The logger of an actor on Scala.js: the browser or Node console. */
private[actor] object PlatformLogger {
  def fromName[F[_]: Sync](name: String): F[Logger[F]] = Sync[F].pure(new ConsoleLogger[F]())
}
