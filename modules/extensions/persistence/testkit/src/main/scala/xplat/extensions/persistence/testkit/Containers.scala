package xplat.extensions.persistence.testkit

import cats.effect.{IO, Resource}

import scala.concurrent.duration._
import scala.sys.process._
import scala.util.Try

/** Starts throwaway containers with the podman or docker CLI, for tests against real databases.
  *
  * The runtime is taken from `XPLAT_CONTAINER_RUNTIME` if set; otherwise podman if installed, else docker. Use fully
  * qualified image names (`docker.io/library/postgres:16`), which both runtimes accept.
  */
object Containers {

  final case class Container(id: String, host: String, port: Int)

  private def works(cli: String): Boolean = Try(Seq(cli, "version").!!(ProcessLogger(_ => ()))).isSuccess

  lazy val runtime: Option[String] =
    sys.env.get("XPLAT_CONTAINER_RUNTIME").filter(_.nonEmpty).orElse(Seq("podman", "docker").find(works))

  /** Whether containers can be started. Tests that need one are canceled otherwise. */
  def available: Boolean = runtime.isDefined

  private def run(command: Seq[String]): String = {
    val err = new StringBuilder
    val out = Try(command.!!(ProcessLogger(_ => (), line => err.append(line).append('\n'))))
    out.getOrElse(throw new IllegalStateException(s"${command.mkString(" ")} failed: $err"))
  }

  /** Runs `image` publishing `port` on a random local port, and waits until `ready` succeeds against it. */
  def container(image: String, port: Int, env: Map[String, String] = Map.empty, args: Seq[String] = Nil)(ready: Container => IO[Unit]): Resource[IO, Container] = {
    val cli = runtime.getOrElse(throw new IllegalStateException("neither podman nor docker is available"))
    val start = IO.blocking {
      val envArgs = env.toSeq.flatMap { case (k, v) => Seq("-e", s"$k=$v") }
      val id = run(Seq(cli, "run", "-d", "--rm", "-p", s"127.0.0.1::$port") ++ envArgs ++ Seq(image) ++ args).linesIterator.toList.last.trim
      val mapped = run(Seq(cli, "port", id, port.toString)).linesIterator.map(_.trim).find(_.startsWith("127.0.0.1:")).getOrElse(
        throw new IllegalStateException(s"port $port of $image is not published")
      )
      Container(id, "127.0.0.1", mapped.split(':').last.toInt)
    }
    val stop = (c: Container) => IO.blocking(Try(run(Seq(cli, "rm", "-f", c.id)))).void

    Resource.make(start)(stop).evalTap(c => retry(ready(c), 120.seconds))
  }

  private def retry(check: IO[Unit], within: FiniteDuration): IO[Unit] =
    IO.monotonic.flatMap { start =>
      def loop: IO[Unit] = check.handleErrorWith { e =>
        IO.monotonic.flatMap(now => if (now - start > within) IO.raiseError(e) else IO.sleep(500.millis) >> loop)
      }
      loop
    }
}
