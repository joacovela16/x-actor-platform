package xplat.flow.engine

import cats.effect.{IO, Ref}
import io.circe.Json
import xplat.flow.model._
import xplat.flow.nodes.NodeType

import scala.concurrent.duration._

/** Nodes for tests that count how many times they ran, can be broken, and can take their time.
  *
  * `test.step` answers `v` with its `value` (its `tag` by default). `test.write` is the same but declares an effect outside,
  * so a simulation must not run it. Their runs are counted under the tag (`write:` and the tag for the second).
  */
final class Probe private (counts: Ref[IO, Map[String, Int]], broken: Ref[IO, Set[String]]) {

  def count(tag: String): IO[Int] = counts.get.map(_.getOrElse(tag, 0))
  def runs: IO[Map[String, Int]] = counts.get
  def breakDown(tag: String): IO[Unit] = broken.update(_ + tag)
  def fix(tag: String): IO[Unit] = broken.update(_ - tag)

  private def config = ConfigSchema(
    List(
      ConfigSchema.Field("tag", DataType.Str, required = false),
      ConfigSchema.Field("value", DataType.AnyJson, required = false),
      ConfigSchema.Field("delayMs", DataType.Num, required = false),
      ConfigSchema.Field("read", DataType.Str, required = false)
    )
  )

  private def reads(params: Json): List[StateRead] =
    params.hcursor.get[String]("read").toOption.flatMap(StatePath.parse(_).toOption).map(StateRead(_, DataType.AnyJson)).toList

  private def make(kindName: String, effects: SideEffects, prefix: String): NodeType[IO] = new NodeType[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor(kindName, "Test", kindName, config, effects, reads = reads)

    def run(input: NodeInput): IO[NodeOutput] = {
      val tag = input.params.hcursor.get[String]("tag").getOrElse("?")
      val delay = input.params.hcursor.get[Long]("delayMs").getOrElse(0L)
      val value = input.params.hcursor.downField("value").focus.getOrElse(Json.fromString(tag))
      counts.update(m => m.updated(prefix + tag, m.getOrElse(prefix + tag, 0) + 1)) >>
        IO.sleep(delay.millis) >>
        broken.get.map(b => if (b(tag)) NodeOutput.failed(s"$tag is broken") else NodeOutput.ok(Map("v" -> value)))
    }
  }

  /** Like `test.step`, with a secret field in its configuration. */
  val secret: NodeType[IO] = new NodeType[IO] {
    val descriptor: NodeDescriptor = NodeDescriptor(
      "test.secret",
      "Test",
      "Secret",
      ConfigSchema(List(ConfigSchema.Field("tag", DataType.Str, required = false), ConfigSchema.Field("apiKey", DataType.Str, secret = true))),
      SideEffects.Pure
    )
    def run(input: NodeInput): IO[NodeOutput] = IO.pure(NodeOutput.ok(Map("v" -> Json.fromString("done"))))
  }

  val step: NodeType[IO] = make("test.step", SideEffects.Pure, "")
  val write: NodeType[IO] = make("test.write", SideEffects.Write, "write:")
}

object Probe {
  def make: IO[Probe] = for {
    counts <- Ref.of[IO, Map[String, Int]](Map.empty)
    broken <- Ref.of[IO, Set[String]](Set.empty)
  } yield new Probe(counts, broken)
}
