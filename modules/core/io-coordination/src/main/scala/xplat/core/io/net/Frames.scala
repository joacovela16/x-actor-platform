package xplat.core.io.net

import cats.effect.kernel.Async
import cats.implicits._
import fs2.Chunk
import fs2.io.net.Socket

import java.nio.ByteBuffer

/** Splits a TCP byte stream into messages: each frame is a 4-byte length followed by that many bytes. */
object Frames {

  /** Frames bigger than this are rejected, so a corrupt length cannot exhaust memory. */
  val MaxFrameSize: Int = 64 * 1024 * 1024

  def encode(payload: Array[Byte]): Chunk[Byte] = {
    val buffer = ByteBuffer.allocate(4 + payload.length)
    buffer.putInt(payload.length).put(payload)
    Chunk.array(buffer.array())
  }

  def write[F[_]](socket: Socket[F], payload: Array[Byte]): F[Unit] = socket.write(encode(payload))

  /** Frames read from the socket until the peer closes it. */
  def read[F[_]](socket: Socket[F])(implicit A: Async[F]): fs2.Stream[F, Array[Byte]] = {
    def exactly(n: Int): F[Option[Chunk[Byte]]] =
      socket.readN(n).map(chunk => if (chunk.size < n) None else Some(chunk))

    val next: F[Option[Array[Byte]]] = exactly(4).flatMap {
      case None => A.pure(None)
      case Some(header) =>
        val size = header.toByteBuffer.getInt
        if (size < 0 || size > MaxFrameSize) A.raiseError(new IllegalStateException(s"invalid frame size $size"))
        else if (size == 0) A.pure(Some(Array.emptyByteArray))
        else exactly(size).map(_.map(_.toArray))
    }

    fs2.Stream.repeatEval(next).unNoneTerminate
  }
}
