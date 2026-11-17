package xplat.flow.engine

import xplat.flow.engine.InstanceState.Status

/** A search of instances. Every condition that is given must hold.
  *
  * @param origin
  *   the start of the label of the origin: `trigger` finds every instance started by a trigger, `trigger:orders` the ones of
  *   that trigger, `subflow:parent-instance` the ones that a given instance started. The labels are `manual`,
  *   `trigger:<id>`, `retry:<id>` and `subflow:<id>`.
  * @param from
  *   instances started at or after this time, and `to` before it (milliseconds since the epoch).
  * @param after
  *   the id of the last instance of the previous page: the search goes on after it.
  */
final case class InstanceQuery(
    flowId: Option[String] = None,
    status: Option[Status] = None,
    origin: Option[String] = None,
    from: Option[Long] = None,
    to: Option[Long] = None,
    limit: Int = 50,
    after: Option[String] = None
)
