package xplat.core.api

import cats.effect.Resource

trait ActorSystemBuilder[F[_]] {
  def build(): Resource[F, ActorSystem[F]]
}
