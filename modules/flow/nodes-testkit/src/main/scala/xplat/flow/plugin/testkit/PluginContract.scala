package xplat.flow.plugin.testkit

import cats.effect.IO
import cats.effect.unsafe.implicits.global
import io.circe.Json
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.model.{NodeInput, Port, SideEffects}

/** The suite every plugin should pass: what the server needs from it beyond loading. Extend it with how to load the plugin and, for
  * each node, inputs that should work:
  * {{{
  * class ExampleContract extends PluginContract {
  *   def plugin = PluginHarness.jvm(jar, manifest)
  *   override def samples = Map("greet" -> List(NodeInput(Json.obj("name" -> "Ana".asJson), Json.obj())))
  * }
  * }}}
  *
  *  - every kind is `<plugin>.<kind>@<major>`, and none is there twice;
  *  - the fields of the configuration, the ports and the outputs have names, each once; `error` is not declared as a port;
  *  - what depends on the params (ports, inputs, outputs, reads, checkParams) answers for any params, also `{}`, `null` or a number;
  *  - a `pure` node run with `{}` answers an output, failed or not, and never throws;
  *  - each sample answers through a port of the node, writes only outputs it declared, and does not fail (unless `failing`).
  */
abstract class PluginContract extends AnyFreeSpec with Matchers {

  /** How to load the plugin: [[PluginHarness.jvm]] or [[PluginHarness.directory]]. */
  def plugin: cats.effect.Resource[IO, PluginHarness]

  /** Inputs that should work, by kind (full or short). */
  def samples: Map[String, List[NodeInput]] = Map.empty

  /** Kinds whose samples are expected to fail (through `error`). */
  def failing: Set[String] = Set.empty

  private def withPlugin[A](f: PluginHarness => A): A = plugin.use(h => IO(f(h))).unsafeRunSync()

  private val strange: List[Json] = List(Json.obj(), Json.Null, Json.fromInt(1), Json.arr(), Json.obj("x" -> Json.obj()))

  "the plugin" - {

    "loads, with every kind named <plugin>.<kind>@<major>, once" in withPlugin { h =>
      h.nodes should not be empty
      val kinds = h.nodes.map(_.descriptor.kind)
      kinds.distinct shouldBe kinds
      all(kinds) should fullyMatch regex s"""${java.util.regex.Pattern.quote(h.manifest.name)}\\.[a-z][a-zA-Z0-9_-]*@${h.manifest.version.major}"""
    }

    "declares fields, ports and outputs with names, once each" in withPlugin { h =>
      h.nodes.foreach { n =>
        val d = n.descriptor
        withClue(s"${d.kind}: ") {
          val fields = d.config.fields.map(_.name)
          all(fields) should not be empty
          fields.distinct shouldBe fields
          val ports = d.ports(Json.obj()).map(_.name)
          ports.distinct shouldBe ports
          ports should not contain Port.Error
          val outputs = d.defaultOutputs(Json.obj()).map(_.name)
          outputs.distinct shouldBe outputs
          d.label.trim should not be empty
        }
      }
    }

    "what depends on the params answers for any params" in withPlugin { h =>
      for (n <- h.nodes; p <- strange) {
        val d = n.descriptor
        withClue(s"${d.kind} with ${p.noSpaces}: ") {
          noException should be thrownBy {
            d.ports(p); d.inputs(p); d.defaultOutputs(p); d.reads(p); d.checkParams(p); d.calls(p); d.config.validate(p)
          }
        }
      }
    }

    "a pure node run with empty params answers, and does not throw" in withPlugin { h =>
      h.nodes.filter(_.descriptor.sideEffects == SideEffects.Pure).foreach { n =>
        withClue(s"${n.descriptor.kind}: ") {
          noException should be thrownBy n.run(NodeInput(Json.obj(), Json.obj())).unsafeRunSync()
        }
      }
    }

    "the samples answer through a port of the node and write only what it declares" in withPlugin { h =>
      for ((kind, inputs) <- samples; input <- inputs) {
        val n = h.node(kind)
        val d = n.descriptor
        withClue(s"${d.kind} with ${input.params.noSpaces}: ") {
          val out = n.run(input).unsafeRunSync()
          if (failing.contains(kind) || failing.contains(d.kind)) out.port shouldBe Port.Error
          else {
            out.error shouldBe None
            d.portNames(input.params) should contain(out.port)
            val declared = (if (input.outputs.nonEmpty) input.outputs else d.defaultOutputs(input.params)).map(_.name).toSet
            out.values.keySet.diff(declared) shouldBe empty
          }
        }
      }
    }
  }
}
