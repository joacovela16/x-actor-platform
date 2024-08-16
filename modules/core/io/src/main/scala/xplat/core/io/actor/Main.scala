package xplat.core.io.actor

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import xplat.core.api.{ActorSystem, Signal}
import xplat.core.io.actor.IOActorSystem._

object Main extends IOApp {

  def run(args: List[String]): IO[ExitCode] = {

    for {
      as <- ActorSystem[IO]
      ref <- as.spawn[Int]("a1") { b =>
        b.message { n =>
          println(n)
          if (n > 0) {
            b.stop
          } else {
            b.same
          }
        }.onSignal { case Signal.OnError(throwable) =>
          b.same
        }
      }
      ref1 <- as.spawn[Int]("a2") { b =>
        b.context { _ =>
          throw new Exception("test")
        }.onSignal { case Signal.OnError(throwable) =>
          b.context { c =>
            c.log.info("Custom stop") as b.stop
          }
        }
      }
      _ <- (0 until 1000).toList.map(x => ref ! x).sequence_
      _ <- as.terminate()
    } yield ExitCode.Success
  }

  /*  as.spawn[Int]("name") { b =>
    b.message { n =>
      b.setup { c =>
        for {
          ref <- c.spawn[Boolean]("inner") { b1 =>
            b1.message {
              case true =>
                b1.same
              case _ =>
                b1.stop
            }

            b1.message { x =>
              if (x) b1.same
              else b1.stop
            }
          }
        } yield b.same
      }
    }.onSignal { x =>
      b.stop
    }
  }.foreach { ref =>
    val c: Option[Unit] = ref ! 4
  }*/

}
