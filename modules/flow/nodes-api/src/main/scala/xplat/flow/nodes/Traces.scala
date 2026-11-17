package xplat.flow.nodes

/** Where a running node is in the trace of its instance, to pass it on: a node that calls another system sends the `traceparent`
  * of its span (W3C Trace Context) with the call, so the trace goes on there. A service of the server for the plugins.
  */
trait Traces[F[_]] {

  /** The `traceparent` of the span of the node `nodeId` of the instance, while it runs; `None` when traces are off. */
  def traceparent(instanceId: String, nodeId: String): F[Option[String]]
}

object Traces {

  /** The header that carries it, in HTTP and in messages. */
  val Header: String = "traceparent"

  def none[F[_]](implicit F: cats.Applicative[F]): Traces[F] = new Traces[F] {
    def traceparent(instanceId: String, nodeId: String): F[Option[String]] = F.pure(None)
  }
}
