package xplat.flow.http

import cats.effect.std.Semaphore
import cats.effect.{Async, Ref, Sync}
import cats.syntax.all._

import java.nio.file.Path

/** Where secret values are kept: the credentials of connections and the keys that triggers check. They are only read by
  * the server, never sent to the UI, and never written to the audit. A secret is named `<connection>.<field>`.
  *
  * With a file they are stored in clear in it, readable only by its owner: protect the data directory like any other file with
  * credentials.
  */
trait SecretStore[F[_]] {

  def get(name: String): F[Option[String]]

  def set(name: String, value: String): F[Unit]

  /** The names, with `prefix` at the start, of the secrets that are set. */
  def names(prefix: String): F[List[String]]

  /** Removes every secret whose name starts with `prefix`. */
  def deleteAll(prefix: String): F[Unit]
}

object SecretStore {

  def make[F[_]: Async](file: Option[Path] = None): F[SecretStore[F]] = inSlot(file.map(JsonSlot.file[F](_, restricted = true)))

  /** The same, kept in any [[JsonSlot]]: one that only the server can read. */
  def inSlot[F[_]: Async](slot: Option[JsonSlot[F]]): F[SecretStore[F]] =
    for {
      initial <- slot.fold(Sync[F].pure(Map.empty[String, String]))(_.read[Map[String, String]](Map.empty))
      ref <- Ref.of[F, Map[String, String]](initial)
      lock <- Semaphore[F](1)
    } yield new SecretStore[F] {

      private def update(f: Map[String, String] => Map[String, String]): F[Unit] =
        lock.permit.use { _ =>
          ref.get.flatMap { all =>
            val next = f(all)
            slot.traverse_(_.write[Map[String, String]](next)) >> ref.set(next)
          }
        }

      def get(name: String): F[Option[String]] = ref.get.map(_.get(name))
      def set(name: String, value: String): F[Unit] = update(_.updated(name, value))
      def names(prefix: String): F[List[String]] = ref.get.map(_.keys.filter(_.startsWith(prefix)).toList.sorted)
      def deleteAll(prefix: String): F[Unit] = update(_.filterNot { case (k, _) => k.startsWith(prefix) })
    }
}
