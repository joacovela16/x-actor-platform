package xplat.core.api

trait Logger[F[_]] {

  def info(message: String): F[Unit]
  def info(message: String, throwable:Throwable): F[Unit]

  def warn(message: String): F[Unit]
  def warn(message: String, throwable:Throwable): F[Unit]

  def error(message: String): F[Unit]
  def error(message: String, throwable:Throwable): F[Unit]

  def debug(message: String): F[Unit]
  def debug(message: String, throwable:Throwable): F[Unit]

}
