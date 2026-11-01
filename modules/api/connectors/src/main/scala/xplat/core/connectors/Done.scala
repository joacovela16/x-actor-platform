package xplat.core.connectors

import xplat.core.api.Codec
import xplat.core.api.error.CodecError

/** Reply of an actor that finished processing a message: the consumer then acknowledges it. */
sealed abstract class Done

case object Done extends Done {

  /** For actors on other nodes (sharded entities, singletons), whose replies travel encoded. */
  implicit val codec: Codec[Done, Array[Byte]] = new Codec[Done, Array[Byte]] {
    def encode(from: Done): Either[CodecError, Array[Byte]] = Right(Array.emptyByteArray)
    def decode(to: Array[Byte]): Either[CodecError, Done] = Right(Done)
  }
}
