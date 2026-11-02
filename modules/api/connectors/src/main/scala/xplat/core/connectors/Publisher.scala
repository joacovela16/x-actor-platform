package xplat.core.connectors

import cats.MonadThrow
import cats.implicits._
import xplat.core.api.{ActorRef, ActorSystem, BehaviorBuilder, ReplyTo}

/** Writes messages to one topic, queue or stream. `publish` completes once the broker has stored the message. */
trait Publisher[F[_], T] {
  def destination: String
  def publish(message: Outgoing[T]): F[Unit]
  def publish(value: T): F[Unit] = publish(Outgoing(value))
}

object Publisher {

  /** Message of a publisher actor. With `replyTo` the sender learns whether the broker stored it (use `ask`). */
  final case class Publish[T](message: Outgoing[T], replyTo: Option[ReplyTo[Result]] = None)

  object Publish {
    def apply[T](value: T): Publish[T] = Publish(Outgoing(value))

    /** For `ask`: `ref.ask[Publisher.Result](Publish.confirmed(Outgoing(order)))`. */
    def confirmed[T](message: Outgoing[T]): ReplyTo[Result] => Publish[T] = replyTo => Publish(message, Some(replyTo))
  }

  sealed trait Result

  object Result {
    case object Published extends Result
    final case class Failed(reason: String) extends Result
  }

  /** An actor that publishes the messages it receives, one at a time and in order. */
  def actor[F[_], T](publisher: Publisher[F, T])(implicit F: MonadThrow[F]): BehaviorBuilder[F, Publish[T]] = b =>
    b.context { ctx =>
      b.message { case Publish(message, replyTo) =>
        publisher.publish(message).attempt.flatMap {
          case Right(_) => F.unit.map(_ => replyTo.foreach(_ ! Result.Published))
          case Left(error) =>
            ctx.log.warn(s"cannot publish to ${publisher.destination}: ${error.getMessage}", error) >>
              F.unit.map(_ => replyTo.foreach(_ ! Result.Failed(String.valueOf(error.getMessage))))
        }.as(b.same)
      }
    }

  def spawn[F[_], T](system: ActorSystem[F], name: String, publisher: Publisher[F, T])(implicit F: MonadThrow[F]): F[ActorRef[F, Publish[T]]] =
    system.spawn[Publish[T]](name)(actor(publisher))
}
