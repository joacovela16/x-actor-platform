package xplat.flow.engine

import io.circe.{Encoder, Json}
import xplat.flow.engine.InstanceState.{NodeStatus, Status}
import xplat.flow.model.StreamSegments

/** What is known about an instance, as the API shows it. */
final case class InstanceStatus(
    id: InstanceId,
    flowId: String,
    revision: Int,
    status: Status,
    state: Json,
    nodes: Map[String, InstanceStatus.Node],
    deadLetters: List[Json] = Nil
)

object InstanceStatus {

  /** @param port
    *   the port a node that finished went on through.
    * @param fireAt
    *   a timer that waits: when it will go on (milliseconds since the epoch).
    */
  final case class Node(status: NodeStatus, port: Option[String], error: Option[String], fireAt: Option[Long] = None, metrics: Option[Json] = None)

  /** The view of an instance that has started. The nodes of a stream segment, which run as one step, show the status of the
    * segment, each with its own metrics: from `live` (the snapshots of the segments that are running, by anchor) while it runs,
    * and from what it said when it ended after that.
    */
  def from(s: InstanceState, live: Map[String, Json] = Map.empty): Option[InstanceStatus] =
    s.flow.map { flow =>
      val own = s.nodes.map { case (id, r) => id -> Node(r.status, r.port, r.error, r.suspended.map(_.until)) }
      val segments = s.graph.toList.flatMap(_.nodes).filter(_.kind == StreamSegments.SegmentKind).map { seg =>
        val run = s.nodes.getOrElse(seg.id, InstanceState.NodeRun())
        val snapshot =
          if (run.status == NodeStatus.Running) live.get(seg.id)
          else run.info.flatMap(i => i.get("metrics").map(m => Json.obj("nodes" -> m, "deadLetters" -> i.getOrElse("deadLetters", Json.arr()))))
        (seg, run, snapshot)
      }
      val members = segments.flatMap { case (seg, run, snapshot) =>
        val perNode = snapshot.flatMap(_.hcursor.downField("nodes").focus).flatMap(_.asObject)
        StreamSegments.membersOf(seg.params).map { m =>
          m.id -> Node(run.status, run.port, run.error, run.suspended.map(_.until), perNode.flatMap(_(m.id)))
        }
      }
      val dead = segments.flatMap { case (_, _, snapshot) => snapshot.flatMap(_.hcursor.downField("deadLetters").focus).flatMap(_.asArray).toList.flatten }
      InstanceStatus(s.id, flow.id, flow.revision, s.status, s.state, own ++ members, dead)
    }

  private def dropNulls(json: Json): Json = json.mapObject(_.filter { case (_, v) => !v.isNull })

  implicit val nodeEncoder: Encoder[Node] = Encoder.instance { n =>
    dropNulls(Json.obj("status" -> Encoder[NodeStatus].apply(n.status), "port" -> Encoder[Option[String]].apply(n.port), "error" -> Encoder[Option[String]].apply(n.error), "fireAt" -> Encoder[Option[Long]].apply(n.fireAt), "metrics" -> Encoder[Option[Json]].apply(n.metrics)))
  }

  implicit val encoder: Encoder[InstanceStatus] = Encoder.instance { s =>
    Json.obj(
      "id" -> Json.fromString(s.id),
      "flowId" -> Json.fromString(s.flowId),
      "revision" -> Json.fromInt(s.revision),
      "status" -> Encoder[Status].apply(s.status),
      "state" -> s.state,
      "nodes" -> Json.fromFields(s.nodes.map { case (id, n) => id -> nodeEncoder(n) }),
      "deadLetters" -> Json.fromValues(s.deadLetters)
    )
  }
}
