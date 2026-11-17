package xplat.flow.http

import cats.effect.Sync
import cats.syntax.all._
import io.circe.{Decoder, Encoder}

import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.attribute.PosixFilePermissions
import java.nio.file.{Files, Path, StandardCopyOption}

/** Reads and writes a value as a JSON file, which is how the stores keep what they hold in the data directory. */
object JsonFile {

  /** The value in the file, or `empty` if the file is not there. */
  def read[F[_]: Sync, A: Decoder](path: Path, empty: => A): F[A] =
    Sync[F].blocking(if (Files.exists(path)) Some(new String(Files.readAllBytes(path), UTF_8)) else None).flatMap {
      case None       => Sync[F].delay(empty)
      case Some(text) => Sync[F].fromEither(io.circe.parser.decode[A](text).leftMap(e => new RuntimeException(s"cannot read $path: ${e.getMessage}", e)))
    }

  /** Replaces the file with the value. It is written aside and moved, so a crash never leaves it half written. With
    * `restricted` only the owner can read it (where the file system has such permissions).
    */
  def write[F[_]: Sync, A: Encoder](path: Path, value: A, restricted: Boolean = false): F[Unit] =
    Sync[F].blocking {
      val absolute = path.toAbsolutePath
      Option(absolute.getParent).foreach(Files.createDirectories(_))
      val tmp = absolute.resolveSibling(absolute.getFileName.toString + ".tmp")
      Files.deleteIfExists(tmp)
      if (restricted) {
        try Files.createFile(tmp, PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
        catch { case _: UnsupportedOperationException => Files.createFile(tmp) }
      }
      Files.write(tmp, Encoder[A].apply(value).spaces2.getBytes(UTF_8))
      try Files.move(tmp, absolute, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE)
      catch { case _: java.nio.file.AtomicMoveNotSupportedException => Files.move(tmp, absolute, StandardCopyOption.REPLACE_EXISTING) }
      ()
    }
}
