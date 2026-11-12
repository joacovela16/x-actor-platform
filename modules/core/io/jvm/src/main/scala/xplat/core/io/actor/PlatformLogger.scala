package xplat.core.io.actor

import cats.effect.Sync
import cats.syntax.functor._
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

/** The logger of an actor on the JVM: SLF4J, so the application picks the backend. */
private[actor] object PlatformLogger {
  def fromName[F[_]: Sync](name: String): F[Logger[F]] = Slf4jLogger.fromName[F](name).map(l => l: Logger[F])
}
