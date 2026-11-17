package xplat.flow.engine

import cats.effect.{Clock, Ref, Sync}
import cats.syntax.all._

import scala.concurrent.duration._

/** The metrics of the instances this server runs, made from the entries of their audit (a [[FlowObserver]]): no node measures
  * itself. It answers them as Prometheus text ([[render]]) and as the health of each flow over the last hours ([[health]]).
  *
  * Labels are the flow, the kind of node, the node and the kind of origin, never the instance, so there are as many series as
  * flows and nodes. The latest finished instances of each flow (up to `keep`, for up to `window`) are kept for the health.
  */
final class FlowMetrics[F[_]] private (state: Ref[F, FlowMetrics.State], keep: Int, window: FiniteDuration)(implicit F: Sync[F])
    extends FlowObserver[F] {

  import FlowMetrics._

  def observe(instanceId: String, entry: AuditEntry): F[Unit] = state.update(_.add(instanceId, entry, keep, window))

  /** The metrics as Prometheus text, with the gauges that others measure (`extra`) at the end. */
  def render(extra: List[Sample] = Nil): F[String] = state.get.map(s => FlowMetrics.render(s, extra))

  /** How each flow went in the last `window` (the latest `keep` instances at most). */
  def health: F[List[FlowHealth]] =
    (state.get, Clock[F].realTime).mapN { (s, now) =>
      val since = now.toMillis - window.toMillis
      val flows = (s.recent.keySet ++ s.running.values.map(_.flowId)).toList.sorted
      flows.map { flow =>
        val done = s.recent.getOrElse(flow, Vector.empty).filter(_.at >= since)
        val durations = done.map(_.durationMs).sorted
        FlowHealth(
          flow,
          running = s.running.values.count(_.flowId == flow),
          succeeded = done.count(_.status == "succeeded"),
          failed = done.count(_.status == "failed"),
          p50Ms = percentile(durations, 0.5),
          p95Ms = percentile(durations, 0.95),
          lastFailures = done.filter(_.status == "failed").sortBy(-_.at).take(5).toList
        )
      }
    }
}

object FlowMetrics {

  def make[F[_]: Sync](keep: Int = 1000, window: FiniteDuration = 24.hours): F[FlowMetrics[F]] =
    Ref.of[F, State](State()).map(new FlowMetrics[F](_, keep, window))

  /** The upper bounds of the buckets of the histograms, in seconds. */
  val Buckets: Vector[Double] = Vector(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300)

  /** A value that someone else measures, for [[render]]. */
  final case class Sample(name: String, help: String, labels: List[(String, String)], value: Double, kind: String = "gauge")

  /** An instance that ended: when, how, how long it took, and why it failed. */
  final case class Finished(instanceId: String, at: Long, status: String, durationMs: Long, error: Option[String])

  final case class FlowHealth(flowId: String, running: Int, succeeded: Int, failed: Int, p50Ms: Option[Long], p95Ms: Option[Long], lastFailures: List[Finished])

  private[engine] final case class Running(flowId: String, error: Option[String])

  private[engine] final case class Histogram(counts: Vector[Long], sum: Double, count: Long) {
    def add(seconds: Double): Histogram =
      Histogram(counts.zip(Buckets).map { case (c, le) => if (seconds <= le) c + 1 else c }, sum + seconds, count + 1)
  }

  private[engine] object Histogram {
    val empty: Histogram = Histogram(Vector.fill(Buckets.size)(0L), 0, 0)
  }

  private type Labels = List[(String, String)]

  private[engine] final case class State(
      running: Map[String, Running] = Map.empty,
      counters: Map[(String, Labels), Long] = Map.empty,
      histograms: Map[(String, Labels), Histogram] = Map.empty,
      recent: Map[String, Vector[Finished]] = Map.empty
  ) {
    private def inc(name: String, labels: Labels): State = copy(counters = counters.updated(name -> labels, counters.getOrElse(name -> labels, 0L) + 1))
    private def observe(name: String, labels: Labels, seconds: Double): State =
      copy(histograms = histograms.updated(name -> labels, histograms.getOrElse(name -> labels, Histogram.empty).add(seconds)))

    /** The flow of an instance; one that started before this server did is `unknown`. */
    private def flowOf(instanceId: String): String = running.get(instanceId).fold("unknown")(_.flowId)

    def add(instanceId: String, entry: AuditEntry, keep: Int, window: FiniteDuration): State = entry match {
      case e: AuditEntry.InstanceStarted =>
        copy(running = running.updated(instanceId, Running(e.flowId, None)))
          .inc("flow_instances_started_total", List("flow" -> e.flowId, "origin" -> e.origin.kind))
      case e: AuditEntry.NodeFinished =>
        val flow = flowOf(instanceId)
        val timed = observe("flow_node_duration_seconds", List("kind" -> e.nodeKind), e.durationMs / 1000.0)
        if (e.status == "failed")
          timed
            .inc("flow_node_failures_total", List("flow" -> flow, "node" -> e.node, "kind" -> e.nodeKind))
            .copy(running = running.updatedWith(instanceId)(_.map(_.copy(error = e.error.map(m => s"${e.node}: $m")))))
        else timed
      case _: AuditEntry.RetryRequested =>
        inc("flow_retries_total", List("flow" -> flowOf(instanceId)))
      case e: AuditEntry.InstanceFinished =>
        val flow = flowOf(instanceId)
        val done = Finished(instanceId, e.at, e.status, e.durationMs, running.get(instanceId).flatMap(_.error))
        val oldest = e.at - window.toMillis
        inc("flow_instances_finished_total", List("flow" -> flow, "status" -> e.status))
          .observe("flow_instance_duration_seconds", List("flow" -> flow), e.durationMs / 1000.0)
          .copy(
            running = running - instanceId,
            recent = recent.updated(flow, (recent.getOrElse(flow, Vector.empty) :+ done).filter(_.at >= oldest).takeRight(keep))
          )
      case _ => this
    }
  }

  private val Help: Map[String, String] = Map(
    "flow_instances_started_total" -> "Instances started, by flow and kind of origin.",
    "flow_instances_finished_total" -> "Instances that ended, by flow and status.",
    "flow_instance_duration_seconds" -> "How long instances took, from start to end.",
    "flow_node_duration_seconds" -> "How long nodes took to run, by kind of node.",
    "flow_node_failures_total" -> "Nodes that failed, by flow, node and kind.",
    "flow_retries_total" -> "Instances retried, by flow.",
    "flow_instances_running" -> "Instances running now on this server, by flow."
  )

  private def escape(v: String): String = v.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")

  private def labelText(labels: Labels): String =
    if (labels.isEmpty) "" else labels.map { case (k, v) => s"""$k="${escape(v)}"""" }.mkString("{", ",", "}")

  private def number(d: Double): String = if (d == d.floor && !d.isInfinite && math.abs(d) < 1e15) d.toLong.toString else d.toString

  private[engine] def render(s: State, extra: List[Sample]): String = {
    val out = new StringBuilder
    def head(name: String, kind: String, help: String): Unit = out ++= s"# HELP $name $help\n# TYPE $name $kind\n"

    s.counters.groupBy(_._1._1).toList.sortBy(_._1).foreach { case (name, series) =>
      head(name, "counter", Help.getOrElse(name, name))
      series.toList.sortBy(_._1._2.toString).foreach { case ((_, labels), v) => out ++= s"$name${labelText(labels)} $v\n" }
    }

    val running = s.running.values.groupBy(_.flowId).view.mapValues(_.size).toList.sortBy(_._1)
    head("flow_instances_running", "gauge", Help("flow_instances_running"))
    running.foreach { case (flow, n) => out ++= s"flow_instances_running${labelText(List("flow" -> flow))} $n\n" }

    s.histograms.groupBy(_._1._1).toList.sortBy(_._1).foreach { case (name, series) =>
      head(name, "histogram", Help.getOrElse(name, name))
      series.toList.sortBy(_._1._2.toString).foreach { case ((_, labels), h) =>
        h.counts.zip(Buckets).foreach { case (c, le) => out ++= s"${name}_bucket${labelText(labels :+ ("le" -> number(le)))} $c\n" }
        out ++= s"${name}_bucket${labelText(labels :+ ("le" -> "+Inf"))} ${h.count}\n"
        out ++= s"${name}_sum${labelText(labels)} ${h.sum}\n"
        out ++= s"${name}_count${labelText(labels)} ${h.count}\n"
      }
    }

    extra.groupBy(_.name).toList.sortBy(_._1).foreach { case (name, samples) =>
      head(name, samples.head.kind, samples.head.help)
      samples.foreach(x => out ++= s"$name${labelText(x.labels)} ${number(x.value)}\n")
    }
    out.toString
  }

  private def percentile(sorted: Vector[Long], p: Double): Option[Long] =
    if (sorted.isEmpty) None else Some(sorted(math.min(sorted.size - 1, math.ceil(p * sorted.size).toInt - 1).max(0)))
}
