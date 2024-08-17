package xplat.core.io.actor

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import xplat.core.api.{ActorSystem, Behavior, Signal}
import xplat.core.io.actor.IOActorSystem._

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    for {
      as <- ActorSystem[IO]
      ref <- as.spawn[Int]("a1") { b =>
        def sum(base: Int): Behavior[IO, Int] = {
          b.message { n =>
            println(base)
            sum(base + n)
          }
        }

        sum(0).onSignal { case Signal.OnError(throwable) =>
          b.same
        }
      }
      ref1 <- as.spawn[Int]("a2") { b =>
        b.context { _ =>
          b.same
        }.onSignal { case Signal.OnError(throwable) =>
          b.context { c =>
            c.log.info("Custom stop") as b.stop
          }
        }
      }
      _ <- (1 until 11).toList.map(x => ref ! x).sequence_
      _ <- as.terminate()
    } yield ExitCode.Success
  }

}
