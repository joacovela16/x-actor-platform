package xplat.flow.stream

import fs2.Stream

/** Where a stream begins: it is started by an ordinary edge and produces elements, until it ends or for ever. */
trait StreamSource[F[_]] extends StreamNodeType[F] {

  /** The elements. Each one carries what to do when the end of the segment is done with it (see [[Elem.ack]]).
    * In a simulation this is only used if the node is [[xplat.flow.model.SideEffects.Pure]], or reads and reads are real; a mock
    * replaces it otherwise.
    */
  def open(ctx: StreamContext[F]): Stream[F, Elem[F]]
}
