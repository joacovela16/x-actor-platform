package xplat.core.api

object Main {

  val as: ActorSystem[Option] = ???

  as.spawn[Int]("name") { b =>
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
  }

}
