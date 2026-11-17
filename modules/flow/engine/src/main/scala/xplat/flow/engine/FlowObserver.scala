package xplat.flow.engine

/** Sees every entry of the audit of every instance this server runs, as it happens (not the ones read back from the journal).
  * For metrics and traces: whatever it does must be quick, and a failure in it is ignored.
  */
trait FlowObserver[F[_]] {
  def observe(instanceId: String, entry: AuditEntry): F[Unit]
}

object FlowObserver {

  /** All of them, in order. */
  def all[F[_]](observers: List[FlowObserver[F]])(implicit F: cats.Monad[F]): FlowObserver[F] = new FlowObserver[F] {
    def observe(instanceId: String, entry: AuditEntry): F[Unit] = cats.syntax.foldable.toFoldableOps(observers).traverse_(_.observe(instanceId, entry))
  }
}
