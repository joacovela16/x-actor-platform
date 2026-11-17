package xplat.flow.stream

import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import fs2.Stream
import io.circe.Json
import xplat.flow.nodes.FlowRunner

import scala.concurrent.duration._

class OperatorsTest extends StreamSpec {

  "map" - {
    val map = new MapOp[IO]

    "sets fields calculated from the element, and removes others" in {
      val params = json("""{"set": {"total": {"mul": [{"path": "price"}, {"path": "qty"}]}, "meta.ok": true}, "remove": ["price"]}""")
      runOperator(map, MapOp.Kind, params, plain(json("""{"price": 2, "qty": 3}"""))).map { case (out, _) =>
        out shouldBe List(json("""{"qty": 3, "total": 6, "meta": {"ok": true}}"""))
      }
    }

    "replaces the element by an expression" in {
      runOperator(map, MapOp.Kind, json("""{"replace": {"path": "n"}}"""), plain(json("""{"n": 4}"""), json("""{"n": 5}"""))).map(_._1 shouldBe numbers(4, 5))
    }

    "an element that cannot be calculated is a dead letter, is counted and acknowledged, and the stream goes on" in {
      for {
        acks <- Ref.of[IO, Int](0)
        r <- runOperator(map, MapOp.Kind, json("""{"set": {"y": {"path": "x"}}}"""), elements(acks, json("""{"x": 1}"""), json("""{"z": 2}"""), json("""{"x": 3}""")))
        (out, h) = r
        dead <- h.dead.get
        n <- acks.get
        m <- h.metrics.snapshot
      } yield {
        out.map(_.hcursor.get[Int]("y").toOption) shouldBe List(Some(1), Some(3))
        dead.size shouldBe 1
        dead.head.hcursor.get[String]("error") shouldBe Right("'x' is not in the element")
        dead.head.hcursor.get[String]("node") shouldBe Right("n")
        n shouldBe 1 // the failed one; the others are acknowledged by whoever ends the stream
        m.hcursor.get[Int]("failed") shouldBe Right(1)
      }
    }

    "bad configuration is reported" in {
      MapOp.params(json("""{"set": {"a": {"bogus": 1}}}""")).isLeft shouldBe true
      MapOp.descriptor.checkParams(json("""{"set": {"a..b": 1}}""")).size shouldBe 1
    }
  }

  "filter" - {
    val filter = new FilterOp[IO]
    val big = json("""{"gt": [{"path": "n"}, 10]}""")

    "keeps what is true and drops the rest, acknowledging and counting what it drops" in {
      for {
        acks <- Ref.of[IO, Int](0)
        r <- runOperator(filter, FilterOp.Kind, Json.obj("where" -> big), elements(acks, json("""{"n": 5}"""), json("""{"n": 50}"""), json("""{"n": 11}""")))
        (out, h) = r
        n <- acks.get
        m <- h.metrics.snapshot
      } yield {
        out.map(_.hcursor.get[Int]("n").toOption) shouldBe List(Some(50), Some(11))
        n shouldBe 1
        m.hcursor.get[Int]("dropped") shouldBe Right(1)
      }
    }

    "a condition that is not true or false is a dead letter" in {
      runOperator(filter, FilterOp.Kind, Json.obj("where" -> json("""{"path": "n"}""")), plain(json("""{"n": 1}"""))).flatMap { case (out, h) =>
        h.dead.get.map { dead => out shouldBe Nil; dead.size shouldBe 1 }
      }
    }
  }

  "flatMap" - {
    val flat = new FlatMapOp[IO]

    "gives one element for each item, and merges the rest of the element if asked" in {
      val input = plain(json("""{"id": 1, "items": [{"sku": "a"}, {"sku": "b"}]}"""))
      for {
        plainItems <- runOperator(flat, FlatMapOp.Kind, json("""{"path": "items"}"""), input)
        merged <- runOperator(flat, FlatMapOp.Kind, json("""{"path": "items", "merge": true}"""), input)
      } yield {
        plainItems._1 shouldBe List(json("""{"sku":"a"}"""), json("""{"sku":"b"}"""))
        merged._1 shouldBe List(json("""{"id":1,"sku":"a"}"""), json("""{"id":1,"sku":"b"}"""))
      }
    }

    "the element is acknowledged with its last item; an empty list drops it; a missing list is a dead letter" in {
      for {
        acks <- Ref.of[IO, Int](0)
        r <- runOperator(flat, FlatMapOp.Kind, json("""{"path": "items"}"""), elements(acks, json("""{"items": [1,2,3]}"""), json("""{"items": []}"""), json("""{"other": 1}""")))
        (out, h) = r
        n <- acks.get
        dead <- h.dead.get
      } yield {
        out shouldBe numbers(1, 2, 3)
        n shouldBe 2 // the empty one and the failed one; the first is acknowledged when its last item is, by whoever ends the stream
        dead.size shouldBe 1
      }
    }
  }

  "dedupe" - {
    val dedupe = new DedupeOp[IO]

    "drops what was seen, by a key or by the whole element" in {
      for {
        byKey <- runOperator(dedupe, DedupeOp.Kind, json("""{"key": "id"}"""), plain(json("""{"id":1,"v":"a"}"""), json("""{"id":2}"""), json("""{"id":1,"v":"b"}""")))
        whole <- runOperator(dedupe, DedupeOp.Kind, json("{}"), plain(json("1"), json("2"), json("1"), json("2"), json("3")))
        dropped <- byKey._2.metrics.snapshot
      } yield {
        byKey._1.map(_.hcursor.get[Int]("id").toOption) shouldBe List(Some(1), Some(2))
        whole._1 shouldBe numbers(1, 2, 3)
        dropped.hcursor.get[Int]("dropped") shouldBe Right(1)
      }
    }

    "forgets the oldest keys beyond max" in {
      runOperator(dedupe, DedupeOp.Kind, json("""{"max": 2}"""), plain(json("1"), json("2"), json("3"), json("1"))).map(_._1 shouldBe numbers(1, 2, 3, 1))
    }
  }

  "throttle" - {
    val throttle = new ThrottleOp[IO]

    "spaces the elements, and with virtual time it takes exactly that" in {
      val program = for {
        start <- IO.monotonic
        r <- runOperator(throttle, ThrottleOp.Kind, json("""{"elements": 1, "per": "1 second"}"""), plain(json("1"), json("2"), json("3")))
        end <- IO.monotonic
      } yield (r._1, end - start)
      TestControl.executeEmbed(program).map { case (out, took) =>
        out shouldBe numbers(1, 2, 3)
        took.toSeconds shouldBe 2L // the first goes at once, then one every second
      }
    }

    "a simulation does not wait" in {
      val program = for {
        start <- IO.monotonic
        r <- runOperator(throttle, ThrottleOp.Kind, json("""{"per": "1 hour"}"""), plain(json("1"), json("2")), simulated = true)
        end <- IO.monotonic
      } yield (r._1, end - start)
      TestControl.executeEmbed(program).map { case (out, took) => out shouldBe numbers(1, 2); took shouldBe Duration.Zero }
    }
  }

  "merge and zip" - {
    "merge gives everything that arrives" in {
      runOperator(new MergeOp[IO], MergeOp.Kind, json("{}"), plain(json("1"), json("2"))).map(_._1 shouldBe numbers(1, 2))
    }

    "zip pairs by position and ends with the shorter side, acknowledging both" in {
      for {
        acksL <- Ref.of[IO, Int](0)
        acksR <- Ref.of[IO, Int](0)
        h <- harness(ZipOp.Kind, json("{}"))
        out <- new ZipOp[IO](
        ).apply(h.ctx, Map("left" -> elements(acksL, json("1"), json("2"), json("3")), "right" -> elements(acksR, json("\"a\""), json("\"b\"")))).evalTap(_.ack).compile.toList
        l <- acksL.get
        r <- acksR.get
      } yield {
        out.map(_.value) shouldBe List(json("""{"left":1,"right":"a"}"""), json("""{"left":2,"right":"b"}"""))
        (l, r) shouldBe ((2, 2))
      }
    }
  }

  "join" - {
    val join = new JoinOp[IO]
    val params = json("""{"leftKey": "id", "rightKey": "orderId"}""")

    def run(left: List[Json], right: List[Json]) =
      harness(JoinOp.Kind, params).flatMap { h =>
        join(h.ctx, Map("left" -> plain(left: _*), "right" -> plain(right: _*))).compile.toList.map(_.map(_.value))
      }

    "pairs the elements that have the same key, whichever side comes first" in {
      run(
        List(json("""{"id": 1, "n": "x"}"""), json("""{"id": 2, "n": "y"}""")),
        List(json("""{"orderId": 2, "p": 20}"""), json("""{"orderId": 1, "p": 10}"""), json("""{"orderId": 9}"""))
      ).map(_.toSet shouldBe Set(
        json("""{"left": {"id": 1, "n": "x"}, "right": {"orderId": 1, "p": 10}}"""),
        json("""{"left": {"id": 2, "n": "y"}, "right": {"orderId": 2, "p": 20}}""")
      ))
    }

    "an element can match several of the other side" in {
      run(List(json("""{"id": 1}""")), List(json("""{"orderId": 1, "k": "a"}"""), json("""{"orderId": 1, "k": "b"}"""))).map(_.size shouldBe 2)
    }

    "an element without its key is a dead letter" in {
      harness(JoinOp.Kind, params).flatMap { h =>
        join(h.ctx, Map("left" -> plain(json("""{"x": 1}""")), "right" -> Stream.empty)).compile.toList.flatMap(out => h.dead.get.map(d => (out, d.size))).map(_ shouldBe ((Nil, 1)))
      }
    }

    "the buffer forgets the oldest keys beyond maxBuffer" in {
      val buffers = (1 to 5).foldLeft(JoinOp.Buffers())((b, i) => b.add(isLeft = true, s"k$i", Json.fromInt(i), 3)._1)
      buffers.left.keySet shouldBe Set("k3", "k4", "k5")
    }
  }

  "sources" - {
    "range gives the numbers, and a list gives the items of the state" in {
      for {
        r <- harness(RangeSource.Kind, json("""{"from": 2, "to": 8, "step": 3}""")).flatMap(h => new RangeSource[IO].open(h.ctx).compile.toList)
        l <- harness(ListSource.Kind, json("""{"path": "items"}""")).flatMap { h =>
          val ctx = new StreamContext[IO](h.ctx.node, h.ctx.input.copy(state = json("""{"items": ["a", "b"]}""")), false, h.metrics, h.dead, h.ctx.now, 10)
          new ListSource[IO].open(ctx).compile.toList
        }
        missing <- harness(ListSource.Kind, json("""{"path": "items"}""")).flatMap(h => new ListSource[IO].open(h.ctx).compile.toList.attempt)
      } yield {
        r.map(_.value) shouldBe numbers(2, 5)
        l.map(_.value) shouldBe List(Json.fromString("a"), Json.fromString("b"))
        missing.isLeft shouldBe true
      }
    }

    "cron ticks come at the times of the expression, and in a simulation without waiting" in {
      val params = json("""{"expression": "0 * * * * *", "maxTicks": 3}""")
      val program = for {
        h <- harness(CronSource.Kind, params)
        start <- IO.monotonic
        ticks <- new CronSource[IO].open(h.ctx).compile.toList
        end <- IO.monotonic
        simulated <- harness(CronSource.Kind, params, simulated = true).flatMap(s => new CronSource[IO].open(s.ctx).compile.toList)
        afterSim <- IO.monotonic
      } yield (ticks.map(_.value.hcursor.get[Long]("at").toOption.get), end - start, simulated.size, afterSim - end)
      TestControl.executeEmbed(program).map { case (ats, took, simulatedCount, simulatedTook) =>
        ats.sliding(2).forall { case Seq(a, b) => b - a == 60000L } shouldBe true
        took should be >= 60.seconds
        simulatedCount shouldBe 3
        simulatedTook shouldBe Duration.Zero
      }
    }

    "the descriptions of the sources have stream outputs and are started by an ordinary edge" in {
      List(ListSource.descriptor, RangeSource.descriptor, CronSource.descriptor, QueueSource.descriptor).foreach { d =>
        d.isSource(Json.obj()) shouldBe true
        d.isSink(Json.obj()) shouldBe false
      }
      succeed
    }
  }

  "evalMap" - {
    /** Answers {"double": n * 2} for the input n, and fails for the ones in `fail`. Slower for small n, to test the order. */
    final class Runner(fail: Set[Int]) extends FlowRunner[IO] {
      def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: FlowRunner.Caller): IO[FlowRunner.Result] = {
        val n = inputs.get("n").flatMap(_.asNumber).flatMap(_.toInt).getOrElse(0)
        IO.sleep(((10 - n) * 30).millis) >> IO.pure {
          if (fail(n)) FlowRunner.Failed("no way") else FlowRunner.Finished(Map("double" -> Json.fromInt(n * 2)))
        }
      }
    }

    "replaces each element by the outputs of the flow, in order, also with several at once" in {
      for {
        one <- runOperator(new EvalMapOp[IO](new Runner(Set.empty)), EvalMapOp.Kind, json("""{"flowId": "c", "inputs": {"n": "$"}}"""), plain(numbers(1, 2, 3): _*))
        par <- runOperator(new EvalMapOp[IO](new Runner(Set.empty)), EvalMapOp.Kind, json("""{"flowId": "c", "inputs": {"n": "$"}, "parallel": 3}"""), plain(numbers(1, 2, 3): _*))
      } yield {
        one._1 shouldBe List(json("""{"double":2}"""), json("""{"double":4}"""), json("""{"double":6}"""))
        par._1 shouldBe one._1
      }
    }

    "with into, sets the outputs inside the element; a flow that fails makes a dead letter" in {
      for {
        r <- runOperator(new EvalMapOp[IO](new Runner(Set(2))), EvalMapOp.Kind, json("""{"flowId": "c", "inputs": {"n": "id"}, "into": "lookup"}"""), plain(json("""{"id": 1}"""), json("""{"id": 2}""")))
        dead <- r._2.dead.get
      } yield {
        r._1 shouldBe List(json("""{"id": 1, "lookup": {"double": 2}}"""))
        dead.size shouldBe 1
        dead.head.hcursor.get[String]("error").toOption.get should include("no way")
      }
    }

    "unordered, the elements leave as their flows end" in {
      runOperator(new EvalMapOp[IO](new Runner(Set.empty)), EvalMapOp.Kind, json("""{"flowId": "c", "inputs": {"n": "$"}, "parallel": 3, "ordered": false}"""), plain(numbers(1, 2, 3): _*))
        .map(_._1 shouldBe List(json("""{"double":6}"""), json("""{"double":4}"""), json("""{"double":2}""")))
    }
  }
}
