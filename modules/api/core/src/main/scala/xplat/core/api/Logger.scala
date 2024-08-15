package xplat.core.api

trait Logger[F[_]] {

  def info(str: Any): F[Unit]
}
