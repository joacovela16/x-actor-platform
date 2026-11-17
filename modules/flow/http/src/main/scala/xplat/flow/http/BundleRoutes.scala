package xplat.flow.http

import cats.effect.Async
import cats.syntax.all._
import io.circe.Json
import io.circe.syntax._
import org.http4s.circe.CirceEntityDecoder._
import org.http4s.dsl.Http4sDsl
import org.http4s.headers.`Content-Type`
import org.http4s.{EntityEncoder, HttpRoutes, MediaType, Response, Status}

/** `/api/bundle`: export flows with what they need, and import them, or see what importing them would do (see [[Bundle]]). */
object BundleRoutes {

  def apply[F[_]](service: Bundle.Service[F])(implicit F: Async[F]): HttpRoutes[F] = {
    val dsl = Http4sDsl[F]
    import dsl._
    import org.http4s.circe.CirceEntityEncoder._

    object Flows extends OptionalQueryParamDecoderMatcher[String]("flows")
    object DryRun extends OptionalQueryParamDecoderMatcher[Boolean]("dryRun")

    HttpRoutes.of[F] {
      case GET -> Root / "api" / "bundle" :? Flows(flows) =>
        val ids = flows.toList.flatMap(_.split(",").map(_.trim).filter(_.nonEmpty))
        service.export(ids).flatMap {
          case Left(problems) => NotFound(ApiJson.problems("not-found", problems))
          // The text is the stable one (keys sorted), so it can be kept in git as it comes.
          case Right(bundle) =>
            F.pure(
              Response[F](Status.Ok)
                .withEntity(Bundle.printer.print(bundle.asJson))(EntityEncoder.stringEncoder[F])
                .withContentType(`Content-Type`(MediaType.application.json))
            )
        }

      case req @ POST -> Root / "api" / "bundle" :? DryRun(dryRun) =>
        req.as[Json].flatMap { body =>
          body.as[Bundle] match {
            case Left(e) => BadRequest(ApiJson.problems("bad-request", List(e.getMessage)))
            case Right(bundle) =>
              if (dryRun.contains(true)) service.plan(bundle).flatMap(p => Ok(p.asJson))
              else
                service.importing(bundle).flatMap {
                  case Left(plan)  => UnprocessableEntity(plan.asJson)
                  case Right(plan) => Ok(plan.asJson)
                }
          }
        }
    }
  }
}
