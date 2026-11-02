package xplat.core.io.cluster

import cats.effect.kernel.Async
import fs2.Pipe
import xplat.core.api.{Codec, Defaults, ReplyTo}
import xplat.core.cluster.api.ClusterRef

import scala.concurrent.duration.FiniteDuration

/** The consumers of [[xplat.core.io.actor.ActorStream]] for refs that may point to another node, such as sharded
  * entities or singletons.
  */
object ClusterStream {

  /** Sends every element to `ref` without waiting. On another node a full mailbox is not reported back, so use [[ask]]
    * to bound how fast the stream sends.
    */
  def sink[F[_], T](ref: ClusterRef[F, T]): Pipe[F, T, Nothing] = _.foreach(ref.send)

  /** Sends each element with a [[Ref]] for the answer, with at most `parallelism` elements waiting for theirs. Emits the
    * answers in the order of the elements. Fails if an answer takes longer than `timeout`.
    */
  def ask[F[_], T, M, B](ref: ClusterRef[F, M], parallelism: Int = 1, timeout: FiniteDuration = Defaults.ASK_TIMEOUT)(f: (T, ReplyTo[B]) => M)(implicit
      A: Async[F],
      codec: Codec[B, Array[Byte]]
  ): Pipe[F, T, B] = {
    _.parEvalMap(parallelism)(element => ref.ask[B](answer => f(element, answer), timeout))
  }

}
