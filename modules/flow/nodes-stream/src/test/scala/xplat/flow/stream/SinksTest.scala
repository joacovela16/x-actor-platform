package xplat.flow.stream

import cats.effect.{IO, Ref}
import com.sun.net.httpserver.{HttpExchange, HttpServer}
import fs2.Stream
import io.circe.Json
import xplat.core.connectors.Publisher
import xplat.flow.model._
import xplat.flow.nodes.{Connections, FlowRunner}
import xplat.flow.queue.PublisherPool

import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets.UTF_8

class SinksTest extends StreamSpec {

  private val out = List(OutputDecl("result", path("r"), DataType.AnyJson))

  private def drain(sink: StreamSink[IO], kind: String, params: Json, input: Stream[IO, Elem[IO]], simulated: Boolean = false): IO[(Map[String, Json], Harness)] =
    harness(kind, params, simulated, out).flatMap(h => sink.drain(h.ctx, input).map(_ -> h))

  "collect" - {
    val collect = new CollectSink[IO]

    "collects the elements, or a field of them, into the first declared output" in {
      for {
        whole <- drain(collect, CollectSink.Kind, json("{}"), plain(numbers(1, 2, 3): _*))
        field <- drain(collect, CollectSink.Kind, json("""{"path": "a"}"""), plain(json("""{"a": 1}"""), json("""{"a": 2}""")))
      } yield {
        whole._1 shouldBe Map("result" -> json("[1,2,3]"))
        field._1 shouldBe Map("result" -> json("[1,2]"))
      }
    }

    "keeps at most limit, drops (and counts) the rest, and acknowledges everything" in {
      for {
        acks <- Ref.of[IO, Int](0)
        r <- drain(collect, CollectSink.Kind, json("""{"limit": 2}"""), elements(acks, numbers(1, 2, 3, 4): _*))
        n <- acks.get
        m <- r._2.metrics.snapshot
      } yield {
        r._1 shouldBe Map("result" -> json("[1,2]"))
        n shouldBe 4
        m.hcursor.get[Int]("dropped") shouldBe Right(2)
      }
    }

    "an element without the field is a dead letter" in {
      drain(collect, CollectSink.Kind, json("""{"path": "a"}"""), plain(json("""{"a": 1}"""), json("""{"b": 2}"""))).flatMap { case (r, h) =>
        h.dead.get.map(d => (r, d.size) shouldBe ((Map("result" -> json("[1]")), 1)))
      }
    }

    "writes nothing if it declares no output" in {
      harness(CollectSink.Kind, json("{}")).flatMap(h => collect.drain(h.ctx, plain(json("1"))).map(_ shouldBe Map.empty))
    }
  }

  "reduce" - {
    val reduce = new ReduceSink[IO]
    val data = plain(json("""{"n": 4}"""), json("""{"n": 10}"""), json("""{"n": 1}"""))

    "counts, sums, averages and finds the extremes" in {
      for {
        count <- drain(reduce, ReduceSink.Kind, json("{}"), data)
        sum <- drain(reduce, ReduceSink.Kind, json("""{"op": "sum", "path": "n"}"""), data)
        avg <- drain(reduce, ReduceSink.Kind, json("""{"op": "avg", "path": "n"}"""), data)
        max <- drain(reduce, ReduceSink.Kind, json("""{"op": "max", "path": "n"}"""), data)
        min <- drain(reduce, ReduceSink.Kind, json("""{"op": "min", "path": "n"}"""), data)
      } yield List(count, sum, avg, max, min).map(_._1("result")) shouldBe List(json("3"), json("15"), json("5"), json("10"), json("1"))
    }

    "any and all turn what the stream found into true or false" in {
      val flags = plain(json("""{"f": true}"""), json("""{"f": false}"""))
      for {
        any <- drain(reduce, ReduceSink.Kind, json("""{"op": "any", "path": "f"}"""), flags)
        all <- drain(reduce, ReduceSink.Kind, json("""{"op": "all", "path": "f"}"""), flags)
        noneAny <- drain(reduce, ReduceSink.Kind, json("""{"op": "any", "path": "f"}"""), Stream.empty)
        noneAll <- drain(reduce, ReduceSink.Kind, json("""{"op": "all", "path": "f"}"""), Stream.empty)
        bad <- drain(reduce, ReduceSink.Kind, json("""{"op": "any", "path": "f"}"""), plain(json("""{"f": 1}""")))
        dead <- bad._2.dead.get
      } yield {
        (any._1("result"), all._1("result"), noneAny._1("result"), noneAll._1("result")) shouldBe ((Json.True, Json.False, Json.False, Json.True))
        dead.size shouldBe 1
      }
    }

    "an empty stream counts 0 and sums 0, but has no minimum or average" in {
      for {
        count <- drain(reduce, ReduceSink.Kind, json("{}"), Stream.empty)
        min <- drain(reduce, ReduceSink.Kind, json("""{"op": "min", "path": "n"}"""), Stream.empty).attempt
      } yield {
        count._1("result") shouldBe json("0")
        min.isLeft shouldBe true
      }
    }

    "a value that is not a number is a dead letter" in {
      drain(reduce, ReduceSink.Kind, json("""{"op": "sum", "path": "n"}"""), plain(json("""{"n": 1}"""), json("""{"n": "x"}"""))).flatMap { case (r, h) =>
        h.dead.get.map(d => (r("result"), d.size) shouldBe ((json("1"), 1)))
      }
    }

    "the configuration is checked" in {
      ReduceSink.params(json("""{"op": "sum"}""")).isLeft shouldBe true
      ReduceSink.params(json("""{"op": "median"}""")).isLeft shouldBe true
    }
  }

  "flow sink" - {
    final class Runner(fail: Set[Int]) extends FlowRunner[IO] {
      val calls = Ref.unsafe[IO, Vector[(String, Option[Int], Map[String, Json], Boolean, FlowRunner.Caller)]](Vector.empty)
      def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: FlowRunner.Caller): IO[FlowRunner.Result] =
        calls.update(_ :+ ((flowId, revision, inputs, wait, caller))) >> IO.pure {
          if (inputs.get("n").flatMap(_.asNumber).flatMap(_.toInt).exists(fail)) FlowRunner.Failed("no way") else FlowRunner.Finished(Map.empty)
        }
    }

    "runs the flow for each element with the inputs filled from it, and counts those that ended well" in {
      val runner = new Runner(Set(2))
      for {
        acks <- Ref.of[IO, Int](0)
        r <- drain(new FlowSink[IO](runner), FlowSink.Kind, json("""{"flowId": "child", "revision": 3, "inputs": {"n": "id", "whole": "$"}}"""), elements(acks, json("""{"id": 1}"""), json("""{"id": 2}"""), json("""{"id": 3}""")))
        calls <- runner.calls.get
        n <- acks.get
        dead <- r._2.dead.get
      } yield {
        r._1 shouldBe Map("result" -> json("2"))
        calls.map(c => (c._1, c._2, c._4, c._5)) should contain only (("child", Some(3), true, FlowRunner.Caller("i", "n")))
        calls.map(_._3("n")).toSet shouldBe Set(json("1"), json("2"), json("3"))
        calls.head._3("whole") shouldBe calls.head._3.apply("whole") // the whole element
        n shouldBe 3
        dead.size shouldBe 1
        dead.head.hcursor.get[String]("error").toOption.get should include("no way")
      }
    }

    "an element that lacks what fills an input is a dead letter, and the flow is not run" in {
      val runner = new Runner(Set.empty)
      drain(new FlowSink[IO](runner), FlowSink.Kind, json("""{"flowId": "child", "inputs": {"n": "id"}}"""), plain(json("""{"other": 1}"""))).flatMap { case (r, h) =>
        (runner.calls.get, h.dead.get).mapN((c, d) => (r("result"), c.size, d.size) shouldBe ((json("0"), 0, 1)))
      }
    }

    "runs several at the same time when asked" in {
      val runner = new Runner(Set.empty)
      drain(new FlowSink[IO](runner), FlowSink.Kind, json("""{"flowId": "c", "parallel": 4, "inputs": {"n": "$"}}"""), plain(numbers(1, 2, 3, 4, 5, 6): _*)).map(_._1("result") shouldBe json("6"))
    }
  }

  "publish sink" - {
    final class Fake extends PublisherPool[IO] {
      val sent = Ref.unsafe[IO, Vector[(String, Option[String], Map[String, String], String)]](Vector.empty)
      def use[A](c: Connections.Resolved, target: String, destinationType: String)(f: Publisher[IO, Array[Byte]] => IO[A]): IO[A] =
        f(new Publisher[IO, Array[Byte]] {
          def destination: String = target
          def publish(m: xplat.core.connectors.Outgoing[Array[Byte]]): IO[Unit] =
            if (new String(m.value, UTF_8).contains("poison")) IO.raiseError(new RuntimeException("broker down"))
            else sent.update(_ :+ ((target, m.key, m.headers, new String(m.value, UTF_8))))
        })
    }
    val connections = new Connections[IO] {
      def resolve(name: String): IO[Option[Connections.Resolved]] = IO.pure(Some(Connections.Resolved(name, "kafka", Json.obj(), Map.empty)).filter(_.name == "kafka-prod"))
    }

    "publishes each element with the key and the idempotency header, and counts them" in {
      val pool = new Fake
      for {
        r <- drain(new PublishSink[IO](connections, pool), PublishSink.Kind, json("""{"connection": "kafka-prod", "destination": "orders", "key": "id", "headers": {"x": "1"}}"""), plain(json("""{"id": "a", "v": 1}"""), json("""{"id": "b", "v": 2}""")))
        sent <- pool.sent.get
      } yield {
        r._1 shouldBe Map("result" -> json("2"))
        sent.map(s => (s._1, s._2, s._4)) shouldBe List(("orders", Some("a"), """{"id":"a","v":1}"""), ("orders", Some("b"), """{"id":"b","v":2}"""))
        sent.map(_._3("x-idempotency-key")) shouldBe List("i:n:1", "i:n:2")
        sent.head._3("x") shouldBe "1"
      }
    }

    "an element that cannot be published is a dead letter and is not counted" in {
      val pool = new Fake
      drain(new PublishSink[IO](connections, pool), PublishSink.Kind, json("""{"connection": "kafka-prod", "destination": "orders"}"""), plain(json("\"ok\""), json("\"poison\""))).flatMap { case (r, h) =>
        h.dead.get.map(d => (r("result"), d.size, d.head.hcursor.get[String]("error").toOption.get.contains("broker down")) shouldBe ((json("1"), 1, true)))
      }
    }

    "fails if the connection does not exist" in {
      drain(new PublishSink[IO](connections, new Fake), PublishSink.Kind, json("""{"connection": "nope", "destination": "orders"}"""), plain(json("1"))).attempt.map(_.isLeft shouldBe true)
    }
  }

  "http sink" - {
    def withServer[A](status: Int => Int)(f: (String, Ref[IO, Vector[String]]) => IO[A]): IO[A] =
      for {
        bodies <- Ref.of[IO, Vector[String]](Vector.empty)
        server <- IO {
          val s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
          s.createContext("/in", (ex: HttpExchange) => {
            // Lo que llega por query (GET/DELETE) se anota como `METHOD ?query`; el resto, el cuerpo.
            val raw = new String(ex.getRequestBody.readAllBytes(), UTF_8)
            val body = Option(ex.getRequestURI.getRawQuery).fold(raw)(q => s"${ex.getRequestMethod} ?$q")
            bodies.update(_ :+ body).unsafeRunSyncCompat()
            val code = status(body.length)
            ex.sendResponseHeaders(code, -1)
            ex.close()
          })
          s.start()
          s
        }
        result <- f(s"http://127.0.0.1:${server.getAddress.getPort}/in", bodies).guarantee(IO(server.stop(0)))
      } yield result

    implicit class Sync(io: IO[Unit]) { def unsafeRunSyncCompat(): Unit = io.unsafeRunSync()(cats.effect.unsafe.implicits.global) }

    "sends each element as JSON and counts the ones that were accepted" in withServer(len => if (len > 10) 500 else 200) { (url, bodies) =>
      drain(new HttpSink[IO], HttpSink.Kind, json(s"""{"url": "$url"}"""), plain(json("""{"a":1}"""), json("""{"long_field": "xxxxxxxxxxxx"}"""), json("""{"a":2}"""))).flatMap { case (r, h) =>
        (bodies.get, h.dead.get).mapN { (b, d) =>
          b.toSet shouldBe Set("""{"a":1}""", """{"long_field":"xxxxxxxxxxxx"}""", """{"a":2}""")
          r("result") shouldBe json("2")
          d.size shouldBe 1
          d.head.hcursor.get[String]("error").toOption.get should include("500")
        }
      }
    }

    "a server that cannot be reached makes dead letters, not a failure of the segment" in {
      drain(new HttpSink[IO], HttpSink.Kind, json("""{"url": "http://127.0.0.1:1/in", "timeoutSeconds": 1}"""), plain(json("1"))).flatMap { case (r, h) =>
        h.dead.get.map(d => (r("result"), d.size) shouldBe ((json("0"), 1)))
      }
    }

    "GET sends each element in the query string, without a body" in withServer(_ => 200) { (url, bodies) =>
      val elements = plain(json("""{"id": 7, "name": "Ana Pérez", "tags": ["a", "b"], "gone": null}"""), json("3"))
      drain(new HttpSink[IO], HttpSink.Kind, json(s"""{"url": "$url?src=flow", "method": "GET"}"""), elements).flatMap { case (r, _) =>
        bodies.get.map { b =>
          b.toSet shouldBe Set("GET ?src=flow&id=7&name=Ana%20P%C3%A9rez&tags=%5B%22a%22%2C%22b%22%5D", "GET ?src=flow&value=3")
          r("result") shouldBe json("2")
        }
      }
    }

    "the URL is checked" in {
      HttpSink.params(json("""{"url": "ftp://x"}""")).isLeft shouldBe true
      HttpSink.params(json("""{"url": "http://x", "method": "CONNECT"}""")).isLeft shouldBe true
      HttpSink.params(json("""{"url": "http://x", "method": "get"}""")).map(_.method) shouldBe Right("GET")
      HttpSink.params(json("""{"url": "http://x"}""")).isRight shouldBe true
    }
  }

  private implicit class Tuple2IO[A, B](t: (IO[A], IO[B])) {
    import cats.syntax.all._
    def mapN[C](f: (A, B) => C): IO[C] = (t._1, t._2).tupled.map(f.tupled)
  }
}
