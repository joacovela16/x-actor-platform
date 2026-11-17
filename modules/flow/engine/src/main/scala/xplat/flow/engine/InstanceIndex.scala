package xplat.flow.engine

/** What the engine keeps to find instances without reading their journals. It is updated when an instance starts and when
  * it ends. The in-memory one is [[InMemoryInstanceIndex]]; a database one can replace it.
  */
trait InstanceIndex[F[_]] {

  /** Records the instance as it is now, replacing what was known of it. */
  def update(summary: InstanceSummary): F[Unit]

  /** The newest instances first; the ones with the same start time by id. */
  def search(query: InstanceQuery): F[Page[InstanceSummary]]
}
