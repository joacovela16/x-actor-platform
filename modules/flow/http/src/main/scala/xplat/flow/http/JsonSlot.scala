package xplat.flow.http

import cats.effect.Sync
import io.circe.{Decoder, Encoder}

import java.nio.file.Path

/** Where a store keeps what it holds, as one JSON value: a file of the data directory, a document of a database... The store
  * reads it once when it starts and replaces it whole every time it changes.
  */
trait JsonSlot[F[_]] {

  /** The value kept, or `empty` if nothing was kept yet. */
  def read[A: Decoder](empty: => A): F[A]

  /** Replaces the value kept, all at once. */
  def write[A: Encoder](value: A): F[Unit]
}

object JsonSlot {

  /** A JSON file (see [[JsonFile]]). With `restricted`, only the owner can read it. */
  def file[F[_]: Sync](path: Path, restricted: Boolean = false): JsonSlot[F] = new JsonSlot[F] {
    def read[A: Decoder](empty: => A): F[A] = JsonFile.read[F, A](path, empty)
    def write[A: Encoder](value: A): F[Unit] = JsonFile.write[F, A](path, value, restricted)
  }
}
