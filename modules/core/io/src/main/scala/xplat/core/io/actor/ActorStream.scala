package xplat.core.io.actor

import cats.effect.kernel.{Async, Deferred}
import cats.implicits._
import fs2.{Pipe, Stream}
import xplat.core.api.{ActorRef, Defaults, Logger, Mailbox, ReplyTo}

import scala.concurrent.duration.FiniteDuration

/** Bridges between actors and fs2 streams.
  *
  *   - [[ActorStream.source]] (publisher): a ref whose messages come out of a stream.
  *   - [[ActorStream.sink]] and [[ActorStream.ask]] (consumers): a stream whose elements go to an actor.
  */
object ActorStream {

  /** A ref and the stream that emits, in order, every message sent to it. `complete` ends the stream once the messages
    * already sent have been emitted; later sends fail. The stream is meant to be run once.
    */
  final case class Source[F[_], T](ref: ActorRef[F, T], stream: Stream[F, T], complete: F[Unit])

  /** Publisher. `mailbox` bounds how many messages may wait for the stream to pull them, with the same overflow policies
    * as an actor mailbox. Use a bounded one when the stream may be slower than the senders.
    */
  def source[F[_], T](name: String, mailbox: Mailbox = Mailbox.Unbounded)(implicit A: Async[F]): F[Source[F, T]] = {
    for {
      box     <- IOMailbox[F, T](mailbox)
      stopped <- Deferred[F, Unit]
      path = s"xplat/stream/$name"
      log <- PlatformLogger.fromName[F](path).map(IOActorSystem.IOLogger.build[F])
      ref = SourceRef[F, T](name, path, box, stopped, log)
    } yield Source(ref, box.stream, box.close >> stopped.complete(()).void)
  }

  /** Consumer: sends every element to `ref`, without waiting for the actor to handle it. With a bounded mailbox its
    * policy applies: `Fail` stops the stream with [[xplat.core.api.error.MailboxOverflow]], and the drop policies may lose
    * elements.
    */
  def sink[F[_], T](ref: ActorRef[F, T]): Pipe[F, T, Nothing] = _.foreach(ref.send)

  /** Consumer with backpressure: each element goes to `ref` with a [[Ref]] for the answer, and the stream does not send
    * more than `parallelism` elements until their answers arrive. Emits the answers in the order of the elements. Fails
    * if an answer takes longer than `timeout`.
    */
  def ask[F[_], T, M, B](ref: ActorRef[F, M], parallelism: Int = 1, timeout: FiniteDuration = Defaults.ASK_TIMEOUT)(f: (T, ReplyTo[B]) => M)(implicit
      A: Async[F]
  ): Pipe[F, T, B] = {
    _.parEvalMap(parallelism)(element => ref.ask[B](answer => f(element, answer), timeout))
  }

  private final case class SourceRef[F[_], T](name: String, path: String, mailbox: IOMailbox[F, T], stopped: Deferred[F, Unit], log: Logger[F])(implicit
      A: Async[F]
  ) extends ActorRef[F, T] {
    override def send(msg: T): F[Unit] = mailbox.send(msg, path, log)
    override def ask[B](f: ReplyTo[B] => T, timeout: FiniteDuration): F[B] = IOAsk(send)(f, timeout)
    override def terminated: F[Unit] = stopped.get
  }

}
