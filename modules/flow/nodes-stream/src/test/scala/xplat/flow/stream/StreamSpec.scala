package xplat.flow.stream

import cats.effect.testing.scalatest.AsyncIOSpec
import cats.effect.{IO, Ref}
import fs2.Stream
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AsyncFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

/** What the tests of the stream nodes share: building elements and the context of a node, and running an operator on a list. */
abstract class StreamSpec extends AsyncFreeSpec with AsyncIOSpec with Matchers {

  protected def json(text: String): Json = parse(text).fold(e => throw e, j => j)
  protected def path(text: String): StatePath = StatePath(text.split('.').toList)

  /** Elements with the acknowledgement counted in `acks`. */
  protected def elements(acks: Ref[IO, Int], values: Json*): Stream[IO, Elem[IO]] =
    Stream.emits(values.map(v => Elem[IO](v, 0L, acks.update(_ + 1))))

  protected def plain(values: Json*): Stream[IO, Elem[IO]] = Stream.emits(values.map(v => Elem.of[IO](v, 0L)))

  protected final case class Harness(ctx: StreamContext[IO], dead: Ref[IO, Vector[Json]], metrics: NodeMetrics[IO])

  protected def harness(kind: String, params: Json, simulated: Boolean = false, outputs: List[OutputDecl] = Nil): IO[Harness] =
    for {
      metrics <- NodeMetrics.make[IO]()
      dead <- Ref.of[IO, Vector[Json]](Vector.empty)
      ctx = new StreamContext[IO](NodeDef("n", kind, params, outputs), NodeInput(Json.obj(), Json.obj(), instanceId = "i", nodeId = "n"), simulated, metrics, dead, IO.realTime.map(_.toMillis), 100)
    } yield Harness(ctx, dead, metrics)

  /** The values that an operator gives for these input elements. */
  protected def runOperator(op: StreamOperator[IO], kind: String, params: Json, input: Stream[IO, Elem[IO]], simulated: Boolean = false): IO[(List[Json], Harness)] =
    harness(kind, params, simulated).flatMap(h => op(h.ctx, Map(Port.In -> input)).compile.toList.map(_.map(_.value) -> h))

  protected def numbers(ns: Int*): List[Json] = ns.toList.map(Json.fromInt)
}
