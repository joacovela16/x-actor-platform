package xplat.core.api

sealed trait Signal {}

object Signal {
  case object PreRestart extends Signal {
    override def hashCode(): Int = 1
  }

  case object PostStop extends Signal {
    override def hashCode(): Int = 2
  }

  final case class StopChild(actorRef: ActorRef[Any, _]) extends Signal {
    override def hashCode(): Int = 3
  }

  final case class OnError(throwable: Throwable) extends Signal {
    override def hashCode(): Int = 4
  }

  final case class OnChillError(throwable: Throwable) extends Signal {
    override def hashCode(): Int = 5
  }

  case object Kill extends Signal
}
