package xplat.flow.engine

import io.circe.Encoder

/** A flow in a list: its latest revision. */
final case class FlowSummary(id: String, name: String, revision: Int)

object FlowSummary {
  implicit val encoder: Encoder[FlowSummary] = Encoder.forProduct3("id", "name", "revision")((f: FlowSummary) => (f.id, f.name, f.revision))
}
