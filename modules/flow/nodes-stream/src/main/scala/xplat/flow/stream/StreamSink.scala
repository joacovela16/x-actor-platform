package xplat.flow.stream

import fs2.Stream
import io.circe.Json

/** Where a stream ends: it consumes the elements until there are no more, and then the execution of the instance goes on. */
trait StreamSink[F[_]] extends StreamNodeType[F] {

  /** Consumes the stream. What it answers is what it writes in the state: by name of the outputs the node declared (see
    * [[StreamContext.result]]). It acknowledges each element once it has dealt with it.
    */
  def drain(ctx: StreamContext[F], upstream: Stream[F, Elem[F]]): F[Map[String, Json]]
}
