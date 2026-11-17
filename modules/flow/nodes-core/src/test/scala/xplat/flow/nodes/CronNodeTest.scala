package xplat.flow.nodes

import cats.Id
import io.circe.Json
import io.circe.parser.parse
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model._

class CronNodeTest extends AnyFreeSpec with Matchers {

  private def json(text: String): Json = parse(text).fold(e => throw e, identity)

  // A calendar where it is always 10 minutes later, so the node can be tested without time zones.
  private val tenMinutes: CronNode.NextFire = (_, _, after) => Right(after + 600000L)
  private val node = new CronNode[Id](tenMinutes)

  private def run(params: String, now: Long = 1000L, entry: Boolean = false) =
    node.run(NodeInput(json(params), Json.obj(), now = now, entry = entry))

  "as a timer it waits until the next time and says when" in {
    val out = run("""{"expression":"*/10 * * * *","timezone":"Europe/Madrid"}""")
    out.suspendUntil shouldBe Some(601000L)
    out.failed shouldBe false
    out.port shouldBe Port.Out
    out.info("fireAt") shouldBe Json.fromLong(601000L)
    out.info("expression") shouldBe Json.fromString("*/10 * * * *")
    out.info("timezone") shouldBe Json.fromString("Europe/Madrid")
  }

  "at the start of a flow it does not wait: the schedule already did" in {
    val out = run("""{"expression":"@daily"}""", entry = true)
    out shouldBe NodeOutput.ok()
    out.suspendUntil shouldBe None
  }

  "maxWait fails the node through the error port when the next time is too far" in {
    run("""{"expression":"@daily","maxWait":"11m"}""").suspendUntil shouldBe Some(601000L)
    val out = run("""{"expression":"@daily","maxWait":"9m"}""")
    out.port shouldBe Port.Error
    out.error.get should include("beyond maxWait 9m")
    run("""{"expression":"@daily","maxWait":"2h"}""").failed shouldBe false
  }

  "a calendar that cannot tell makes the timer fail, and a bad configuration too" in {
    val none = new CronNode[Id](CronNode.NoClock)
    none.run(NodeInput(json("""{"expression":"@daily"}"""), Json.obj())).error.get should include("cannot compute")
    // A launcher does not need the calendar to run.
    none.run(NodeInput(json("""{"expression":"@daily"}"""), Json.obj(), entry = true)).failed shouldBe false
    run("""{"expression":"61 * * * *"}""").error.get should include("minute: 61 is out of range")
    run("""{}""").failed shouldBe true
    run("""{"expression":"@daily","maxWait":"soon"}""").failed shouldBe true
    new CronNode[Id]((_, _, _) => Left("no such zone")).run(NodeInput(json("""{"expression":"@daily"}"""), Json.obj())).error shouldBe Some("no such zone")
  }

  "durations" in {
    CronNode.duration("90s") shouldBe Right(90000L)
    CronNode.duration("30m") shouldBe Right(1800000L)
    CronNode.duration("2h") shouldBe Right(7200000L)
    CronNode.duration("1d") shouldBe Right(86400000L)
    CronNode.duration("m").isLeft shouldBe true
    CronNode.duration("10").isLeft shouldBe true
    CronNode.duration("1w").isLeft shouldBe true
  }

  "configuration is validated when the flow is saved" in {
    val d = CronNode.descriptor
    d.config.validate(json("""{"expression":"@daily"}""")) shouldBe Nil
    d.config.validate(json("""{"expression":"0 9 * * MON-FRI","timezone":"America/Argentina/Buenos_Aires","misfire":"skip","maxWait":"1d","inputs":{"x":1}}""")) shouldBe Nil
    d.config.validate(json("{}")) shouldBe List("missing field 'expression'")
    d.config.validate(json("""{"expression":"@daily","misfire":"later"}""")).size shouldBe 1
    d.checkParams(json("""{"expression":"@daily"}""")) shouldBe Nil
    d.checkParams(json("""{"expression":"* * *"}""")).head should include("expression: a cron expression has 5 or 6 fields")
    d.checkParams(json("""{"expression":"@daily","timezone":"not a zone!"}""")).head should include("timezone")
    d.checkParams(json("""{"expression":"@daily","maxWait":"x"}""")).head should include("maxWait")
    d.checkParams(json("""{"expression":"@daily","inputs":[1]}""")) shouldBe List("inputs must be an object")
    d.sideEffects shouldBe SideEffects.Pure
    d.portNames(Json.obj()) shouldBe List("out", "error")
  }

  "a flow with a cron node is validated against the core catalog" in {
    val flow = FlowDef("f", "F", List(NodeDef("tick", CronNode.Kind, json("""{"expression":"*/5 * * * *"}""")), NodeDef("after", CronNode.Kind, json("""{"expression":"@hourly"}"""))), List(Edge("tick", "after")))
    FlowValidator.validate(flow, CoreNodes.catalog) shouldBe Nil
    FlowValidator.validate(flow.copy(nodes = flow.nodes.updated(0, NodeDef("tick", CronNode.Kind, json("""{"expression":"x"}""")))), CoreNodes.catalog).map(_.code) shouldBe List("invalid-params")
  }

  "the registry has it" in {
    CoreNodes.registry[Id](new FlowRunner[Id] {
      def run(flowId: String, revision: Option[Int], inputs: Map[String, Json], wait: Boolean, caller: FlowRunner.Caller): FlowRunner.Result = FlowRunner.Finished(Map.empty)
    }).get(CronNode.Kind) should not be empty
  }
}
