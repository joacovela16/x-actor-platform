package xplat.flow.http

import cats.effect.Async
import fs2.io.file.Files
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Cache-Control`
import org.http4s.CacheDirective
import org.http4s.{HttpRoutes, Response, StaticFile, Status}

import scala.concurrent.duration._

/** The files of the UI, from the classpath. A path without an extension that is not a file is a route of the UI, which gets
  * `index.html`; a missing file with an extension is a 404. If the UI is not in the classpath the answer is 503.
  *
  * `index.html` is always asked again (`no-cache`), so a new build of the UI shows on the next reload; the files of `assets/` have
  * the hash of their content in the name, so they are kept for a year.
  */
object StaticRoutes {

  def apply[F[_]: Async](root: String = "/public"): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._
    implicit val files: Files[F] = Files.forAsync[F]

    def index(req: org.http4s.Request[F]): F[Response[F]] =
      StaticFile
        .fromResource[F](s"$root/index.html", Some(req))
        .map(_.putHeaders(`Cache-Control`(CacheDirective.`no-cache`())))
        .getOrElseF(
          Async[F].pure(Response[F](Status.ServiceUnavailable).withEntity(ApiJson.problems("ui-not-built", List("the UI is not in this build: run npm in modules/flow/ui or build without -Dflow.ui.skip"))))
        )

    HttpRoutes.of[F] {
      case req @ GET -> path if !path.startsWith(Root / "api") =>
        val segments = path.segments.map(_.decoded())
        if (segments.exists(s => s == ".." || s == "." || s.contains('\\'))) NotFound()
        else if (segments.isEmpty) index(req)
        else {
          val relative = segments.mkString("/")
          val cache =
            if (segments.head == "assets") `Cache-Control`(CacheDirective.public, CacheDirective.`max-age`(365.days), CacheDirective("immutable"))
            else `Cache-Control`(CacheDirective.`no-cache`())
          StaticFile.fromResource[F](s"$root/$relative", Some(req)).map(_.putHeaders(cache)).getOrElseF {
            if (segments.last.contains('.')) NotFound() else index(req)
          }
        }
    }
  }
}
