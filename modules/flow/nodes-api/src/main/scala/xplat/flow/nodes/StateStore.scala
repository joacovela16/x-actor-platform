package xplat.flow.nodes

import cats.Applicative
import io.circe.Json

/** Where nodes keep, by key, what has to outlive a restart of the server: the state of the operators of a stream segment, so that
  * a segment that is run again after a restart goes on from where it was (see `SegmentNode`). The server keeps it where it keeps
  * the rest of its state.
  */
trait StateStore[F[_]] {
  def get(key: String): F[Option[Json]]
  def put(key: String, value: Json): F[Unit]
  def delete(key: String): F[Unit]
}

object StateStore {

  /** Keeps nothing: what is put is lost. For tests and tools. */
  def none[F[_]](implicit F: Applicative[F]): StateStore[F] = new StateStore[F] {
    def get(key: String): F[Option[Json]] = F.pure(None)
    def put(key: String, value: Json): F[Unit] = F.unit
    def delete(key: String): F[Unit] = F.unit
  }
}
