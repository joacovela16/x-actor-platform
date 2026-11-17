package xplat.flow.server

import cats.effect.std.{Console, Queue}
import cats.effect.{Async, Resource}
import cats.syntax.all._
import io.opentelemetry.api.common.{AttributeKey, Attributes}
import io.opentelemetry.api.trace.{SpanContext, SpanKind, TraceFlags, TraceState}
import io.opentelemetry.exporter.otlp.http.trace.OtlpHttpSpanExporter
import io.opentelemetry.sdk.common.{InstrumentationLibraryInfo, InstrumentationScopeInfo}
import io.opentelemetry.sdk.resources.{Resource => OtelResource}
import io.opentelemetry.sdk.trace.data.{EventData, LinkData, SpanData, StatusData}
import io.opentelemetry.sdk.trace.export.SpanExporter
import xplat.flow.engine.FlowTracing

import java.util.concurrent.TimeUnit
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._

/** Sends the spans of [[FlowTracing]] to an OpenTelemetry collector by OTLP/HTTP, in batches (up to 512, or every 2 s). What
  * is still waiting when the server stops is sent then. A collector that does not answer loses those spans, never the instances.
  */
object OtlpTraces {

  def resource[F[_]: Async: Console](config: ServerConfig.Otel): Resource[F, List[FlowTracing.Span] => F[Unit]] =
    Resource
      .make(Async[F].delay(OtlpHttpSpanExporter.builder().setEndpoint(s"${config.endpoint}/v1/traces").build(): SpanExporter))(e =>
        Async[F].blocking(e.shutdown().join(5, TimeUnit.SECONDS)).void
      )
      .flatMap(exporter => sender[F](exporter, config.service))

  /** Spans go to `exporter` in batches, sent by a fiber of their own. */
  def sender[F[_]](exporter: SpanExporter, service: String, every: FiniteDuration = 2.seconds, batch: Int = 512)(implicit
      F: Async[F],
      C: Console[F]
  ): Resource[F, List[FlowTracing.Span] => F[Unit]] = {
    val otelResource = OtelResource.getDefault.merge(OtelResource.create(Attributes.of(AttributeKey.stringKey("service.name"), service)))

    def send(spans: List[FlowTracing.Span]): F[Unit] =
      if (spans.isEmpty) F.unit
      else
        F.blocking(exporter.`export`(spans.map(toOtel(_, otelResource)).asJava).join(10, TimeUnit.SECONDS)).flatMap { r =>
          C.errorln(s"traces: the collector did not take ${spans.size} spans").unlessA(r.isSuccess)
        }.handleErrorWith(e => C.errorln(s"traces: cannot send ${spans.size} spans: ${e.getMessage}"))

    for {
      queue <- Resource.eval(Queue.unbounded[F, FlowTracing.Span])
      loop = (F.sleep(every) >> queue.tryTakeN(Some(batch)).flatMap(send)).foreverM[Unit]
      _ <- F.background(loop)
      // What is still waiting goes out when the server stops.
      _ <- Resource.onFinalize(queue.tryTakeN(None).flatMap(_.grouped(batch).toList.traverse_(send)))
    } yield (spans: List[FlowTracing.Span]) => spans.traverse_(queue.offer)
  }

  /** A span of the flow as OpenTelemetry sends it. */
  def toOtel(span: FlowTracing.Span, otelResource: OtelResource): SpanData = {
    val context = SpanContext.create(span.traceId, span.spanId, TraceFlags.getSampled, TraceState.getDefault)
    val parent = span.parentSpanId.fold(SpanContext.getInvalid)(p => SpanContext.create(span.traceId, p, TraceFlags.getSampled, TraceState.getDefault))
    val attributes = span.attributes.toList.sortBy(_._1).foldLeft(Attributes.builder())((b, kv) => b.put(kv._1, kv._2)).build()
    val status = span.error.fold(StatusData.ok())(StatusData.create(io.opentelemetry.api.trace.StatusCode.ERROR, _))
    val kind = span.kind match {
      case FlowTracing.Span.Server   => SpanKind.SERVER
      case FlowTracing.Span.Internal => SpanKind.INTERNAL
    }
    new SpanData {
      def getName: String = span.name
      def getKind: SpanKind = kind
      def getSpanContext: SpanContext = context
      def getParentSpanContext: SpanContext = parent
      def getStatus: StatusData = status
      def getStartEpochNanos: Long = TimeUnit.MILLISECONDS.toNanos(span.startMs)
      def getAttributes: Attributes = attributes
      def getEvents: java.util.List[EventData] = java.util.Collections.emptyList()
      def getLinks: java.util.List[LinkData] = java.util.Collections.emptyList()
      def getEndEpochNanos: Long = TimeUnit.MILLISECONDS.toNanos(span.endMs)
      def hasEnded: Boolean = true
      def getTotalRecordedEvents: Int = 0
      def getTotalRecordedLinks: Int = 0
      def getTotalAttributeCount: Int = attributes.size()
      @deprecated("use getInstrumentationScopeInfo", "")
      def getInstrumentationLibraryInfo: InstrumentationLibraryInfo = InstrumentationLibraryInfo.create(Scope, null)
      override def getInstrumentationScopeInfo: InstrumentationScopeInfo = InstrumentationScopeInfo.create(Scope)
      def getResource: OtelResource = otelResource
    }
  }

  private val Scope = "xplat.flow"
}
