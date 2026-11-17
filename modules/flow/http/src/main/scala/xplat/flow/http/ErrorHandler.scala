package xplat.flow.http

import cats.data.{Kleisli, OptionT}
import cats.effect.Sync
import cats.effect.std.Console
import cats.syntax.all._
import org.http4s.circe.CirceEntityEncoder._
import org.http4s.{HttpRoutes, MessageFailure, Response, Status}
import xplat.flow.engine.FlowEngine

/** Turns the errors that a route raises into answers in JSON: what the engine says is the fault of the request (a flow that does
  * not exist, inputs that do not fit) is a 4xx, and anything else is a 500 that is logged.
  */
object ErrorHandler {

  def apply[F[_]: Sync: Console](routes: HttpRoutes[F]): HttpRoutes[F] =
    Kleisli(req => OptionT(routes.run(req).value.handleErrorWith(e => answer(e).map(Option(_)))))

  private def answer[F[_]: Sync: Console](error: Throwable): F[Response[F]] = {
    def reply(status: Status, code: String, message: String) = Sync[F].pure(Response[F](status).withEntity(ApiJson.problems(code, List(message))))
    error match {
      case e: FlowEngine.FlowNotFound     => reply(Status.NotFound, "not-found", e.getMessage)
      case e: FlowEngine.InstanceNotFound => reply(Status.NotFound, "not-found", e.getMessage)
      case e: FlowEngine.InvalidInputs    => reply(Status.UnprocessableContent, "invalid-inputs", e.getMessage)
      case e: FlowEngine.NotFinished      => reply(Status.Conflict, "not-finished", e.getMessage)
      case e: MessageFailure              => reply(Status.BadRequest, "bad-request", e.getMessage)
      case e =>
        Console[F].errorln(s"flow http: unexpected error: $e") >> reply(Status.InternalServerError, "internal-error", "unexpected error")
    }
  }
}
