package xplat.core.io.actor

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import xplat.core.api.{ActorRef, ActorSystem, Behaviors, Signal}
import xplat.core.io.actor.IOActorSystemBuilder.ioActorSystem
import xplat.core.api.ReplyTo

object Main extends IOApp {

  final case class Ping()
  final case class Pong(respondTo: ReplyTo[String])

  def run(args: List[String]): IO[ExitCode] = {

    ActorSystem[IO].use { as =>
      for {
        ref <- as.spawn[Int]("a1")(
          Behaviors.message { n =>
            println(n)
            if (n < 10) {
              Behaviors.same
            } else
              Behaviors.stop
          }
        )

        _ <- as.spawn[Int]("a2")(
          Behaviors.onSignal(
            Behaviors.setup(_ => throw new Exception(">>")),
            { case Signal.OnError(throwable) =>
              Behaviors.setup { c =>
                Behaviors.pipe(c.log.error(throwable.getLocalizedMessage, throwable).as(Behaviors.stop))
              }
            }
          )
        )
        _ <- (1 until 3).toList.map(x => ref ! x).sequence_
      } yield ExitCode.Success
    }

    /*ActorSystem[IO].use { as =>
      for {
        ping <- as.spawn[Pong]("ping") { b =>
          b.message { pong =>
            pong.respondTo ! "pepe"
          }
        }
        r1 <- ping.ask[String](ref => Pong(ref))
        r2 <- ping.ask[String](ref => Pong(ref))
        _ = println(r1)
        _ = println(r2)
      } yield ExitCode.Success
    }*/
  }

}
