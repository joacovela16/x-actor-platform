package xplat.flow.engine

import cats.effect.Temporal
import cats.syntax.applicative._
import cats.syntax.applicativeError._
import cats.syntax.flatMap._
import cats.syntax.functor._
import xplat.flow.engine.InstanceMachine.Launch
import xplat.flow.model.NodeOutput
import xplat.flow.nodes.NodeRegistry

import java.util.concurrent.TimeoutException
import scala.concurrent.duration.FiniteDuration

/** Runs a node that is ready. An error, an exception or running out of time is an answer, not a crash: the node fails. */
private[engine] object NodeExecutor {

  def run[F[_]](registry: NodeRegistry[F], timeout: FiniteDuration)(launch: Launch)(implicit F: Temporal[F]): F[Either[String, NodeOutput]] =
    registry.get(launch.kind) match {
      case None => (Left(s"there is no node of kind '${launch.kind}'"): Either[String, NodeOutput]).pure[F]
      case Some(nodeType) =>
        (if (nodeType.unbounded) F.unit.flatMap(_ => nodeType.run(launch.input)) else F.timeout(F.unit.flatMap(_ => nodeType.run(launch.input)), timeout)).attempt.map {
          case Right(output)              => Right(output)
          case Left(_: TimeoutException) => Left(s"it did not finish in $timeout")
          case Left(error)                => Left(Option(error.getMessage).getOrElse(error.toString))
        }
    }
}
