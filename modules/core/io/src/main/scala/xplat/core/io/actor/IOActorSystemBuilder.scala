package xplat.core.io.actor

import cats.effect.kernel.Async
import xplat.core.api.ActorSystemBuilder

object IOActorSystemBuilder {

  implicit def ioActorSystem[F[_]](implicit A: Async[F]): ActorSystemBuilder[F] = { () =>
    IOActorSystem("root")
  }

}
