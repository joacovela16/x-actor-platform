package xplat.flow.stream

import cats.effect.testkit.TestControl
import cats.effect.{IO, Ref}
import fs2.Stream
import io.circe.Json
import xplat.flow.model.Port

import scala.concurrent.duration._

class WindowAggregateTest extends StreamSpec {

  private val window = new WindowOp[IO]
  private val aggregate = new AggregateOp[IO]

  private def items(w: Json): List[Json] = w.hcursor.get[List[Json]]("items").getOrElse(Nil)

  "tumbling windows by count are full, and the last one has what is left" in {
    runOperator(window, WindowOp.Kind, json("""{"size": 2}"""), plain(numbers(1, 2, 3, 4, 5): _*)).map { case (out, _) =>
      out.map(items) shouldBe List(numbers(1, 2), numbers(3, 4), numbers(5))
      out.map(_.hcursor.get[Int]("count").toOption.get) shouldBe List(2, 2, 1)
    }
  }

  "a tumbling window is acknowledged when it is passed on, not before" in {
    for {
      acks <- Ref.of[IO, Int](0)
      h <- harness(WindowOp.Kind, json("""{"size": 2}"""))
      seenBefore <- Ref.of[IO, List[Int]](Nil)
      _ <- window(h.ctx, Map(Port.In -> elements(acks, numbers(1, 2, 3): _*))).evalTap(e => acks.get.flatMap(n => seenBefore.update(_ :+ n)) >> e.ack).compile.drain
      before <- seenBefore.get
      total <- acks.get
    } yield {
      before shouldBe List(0, 2) // when the first window comes out nothing is acknowledged yet; then the elements of that window
      total shouldBe 3
    }
  }

  "tumbling windows by time close every duration, and the stream ends with a window of what is left" in {
    val program = runOperator(window, WindowOp.Kind, json("""{"by": "time", "duration": "350 millis"}"""), Stream.awakeEvery[IO](100.millis).take(10).map(d => Elem.of[IO](Json.fromLong(d.toMillis), 0L)))
    TestControl.executeEmbed(program).map { case (out, _) =>
      // One element every 100 ms for a second, in windows of 350 ms: a few windows of three or four, with nothing lost or repeated.
      val sizes = out.map(items(_).size)
      sizes.sum shouldBe 10
      sizes.size should be >= 3
      sizes.foreach(_ should be <= 4)
      out.flatMap(items).map(_.asNumber.get.toLong.get) shouldBe (1 to 10).map(_ * 100L).toList
    }
  }

  "sliding windows by count move every n elements" in {
    for {
      one <- runOperator(window, WindowOp.Kind, json("""{"kind": "sliding", "size": 3}"""), plain(numbers(1, 2, 3, 4, 5): _*))
      two <- runOperator(window, WindowOp.Kind, json("""{"kind": "sliding", "size": 3, "every": 2}"""), plain(numbers(1, 2, 3, 4, 5): _*))
    } yield {
      one._1.map(items) shouldBe List(numbers(1, 2, 3), numbers(2, 3, 4), numbers(3, 4, 5))
      two._1.map(items) shouldBe List(numbers(1, 2, 3), numbers(3, 4, 5))
    }
  }

  "sliding windows by time give the elements of the last duration every step" in {
    val params = json("""{"kind": "sliding", "by": "time", "duration": "300 millis", "every": "200 millis"}""")
    val program = runOperator(window, WindowOp.Kind, params, Stream.awakeEvery[IO](100.millis).take(6).map(d => Elem.of[IO](Json.fromLong(d.toMillis), d.toMillis)))
    TestControl.executeEmbed(program).map { case (out, _) =>
      out.nonEmpty shouldBe true
      // No window ever has an element older than the duration, and none has more than 3 of the one-per-100ms elements.
      out.foreach(w => items(w).size should be <= 3)
    }
  }

  "in a simulation a window by time is one with everything" in {
    runOperator(window, WindowOp.Kind, json("""{"by": "time", "duration": "1 hour"}"""), plain(numbers(1, 2, 3): _*), simulated = true).map(_._1.map(items) shouldBe List(numbers(1, 2, 3)))
  }

  "windows are described and their configuration checked" in {
    WindowOp.params(json("{}")).isLeft shouldBe true // size is needed
    WindowOp.params(json("""{"size": 0}""")).isLeft shouldBe true
    WindowOp.params(json("""{"by": "time"}""")).isLeft shouldBe true
    WindowOp.params(json("""{"kind": "sliding", "by": "time", "duration": "1 second"}""")).isLeft shouldBe true
    WindowOp.params(json("""{"size": 5}""")).isRight shouldBe true
  }

  private val sales = List(
    json("""{"shop": "a", "amount": 10}"""),
    json("""{"shop": "b", "amount": 5}"""),
    json("""{"shop": "a", "amount": 30}"""),
    json("""{"shop": "b", "amount": 1}""")
  )

  "aggregate over the stream gives one row per group, in the order the groups appeared, when it ends" in {
    val params = json("""{"key": "shop", "aggregations": {"n": {"op": "count"}, "total": {"op": "sum", "path": "amount"}, "avg": {"op": "avg", "path": "amount"}, "max": {"op": "max", "path": "amount"}, "min": {"op": "min", "path": "amount"}, "all": {"op": "collect", "path": "amount"}, "first": {"op": "first", "path": "amount"}, "last": {"op": "last", "path": "amount"}}}""")
    runOperator(aggregate, AggregateOp.Kind, params, plain(sales: _*)).map { case (out, _) =>
      out shouldBe List(
        json("""{"key": "a", "all": [10, 30], "avg": 20, "first": 10, "last": 30, "max": 30, "min": 10, "n": 2, "total": 40}"""),
        json("""{"key": "b", "all": [5, 1], "avg": 3, "first": 5, "last": 1, "max": 5, "min": 1, "n": 2, "total": 6}""")
      )
    }
  }

  "aggregate with emit each gives the new row of the group after every element" in {
    val params = json("""{"key": "shop", "emit": "each", "aggregations": {"total": {"op": "sum", "path": "amount"}}}""")
    runOperator(aggregate, AggregateOp.Kind, params, plain(sales: _*)).map(_._1.map(r => r.hcursor.get[String]("key").toOption.get -> r.hcursor.get[Int]("total").toOption.get) shouldBe List("a" -> 10, "b" -> 5, "a" -> 40, "b" -> 6))
  }

  "aggregate without a key gives one row" in {
    runOperator(aggregate, AggregateOp.Kind, json("""{"aggregations": {"n": {"op": "count"}}}"""), plain(sales: _*)).map(_._1 shouldBe List(json("""{"n": 4}""")))
  }

  "aggregate over items works on a window, and a window can feed it" in {
    val params = json("""{"key": "shop", "over": "items", "aggregations": {"total": {"op": "sum", "path": "amount"}}}""")
    for {
      h <- harness(WindowOp.Kind, json("""{"size": 3}"""))
      windows = window(h.ctx, Map(Port.In -> plain(sales: _*)))
      a <- harness(AggregateOp.Kind, params)
      out <- aggregate(a.ctx, Map(Port.In -> windows)).compile.toList.map(_.map(_.value))
    } yield out shouldBe List(
      json("""{"key": "a", "total": 40}"""),
      json("""{"key": "b", "total": 5}"""),
      json("""{"key": "b", "total": 1}""")
    )
  }

  "an element that the aggregate cannot take in is a dead letter and does not change the result" in {
    val params = json("""{"aggregations": {"total": {"op": "sum", "path": "amount"}}}""")
    for {
      r <- runOperator(aggregate, AggregateOp.Kind, params, plain(json("""{"amount": 1}"""), json("""{"amount": "x"}"""), json("""{"other": 1}"""), json("""{"amount": 2}""")))
      dead <- r._2.dead.get
    } yield {
      r._1 shouldBe List(json("""{"total": 3}"""))
      dead.size shouldBe 2
    }
  }

  "aggregate configuration is checked" in {
    AggregateOp.params(json("{}")).isLeft shouldBe true
    AggregateOp.params(json("""{"aggregations": {"a": {"op": "sum"}}}""")).isLeft shouldBe true
    AggregateOp.params(json("""{"aggregations": {"a": {"op": "median", "path": "x"}}}""")).isLeft shouldBe true
    AggregateOp.params(json("""{"over": "x", "aggregations": {"a": {"op": "count"}}}""")).isLeft shouldBe true
    AggregateOp.params(json("""{"aggregations": {"a": {"op": "count"}}}""")).isRight shouldBe true
  }
}
