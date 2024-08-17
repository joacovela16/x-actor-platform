package xplat.core.io.actor

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import xplat.core.api.{ActorSystem, Signal}
import xplat.core.io.actor.IOActorSystemBuilder.ioActorSystem

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    ActorSystem[IO].use { as =>
      for {
        ref <- as.spawn[Int]("a1") { b =>
          b.message { n =>
            println(n)
            if (n < 10) {
              b.same
            } else
              b.stop
          }
        }

        ref1 <- as.spawn[Int]("a2") { b =>
          b.context { _ =>
            throw new Exception(">>")
          }.onSignal { case Signal.OnError(throwable) =>
            b.context { c =>
              c.log.error(throwable.getLocalizedMessage, throwable) as b.stop
            }
          }
        }
        _ <- (1 until 3).toList.map(x => ref ! x).sequence_
      } yield ExitCode.Success
    }
  }

}
