package xplat.core.connectors

/** A message to publish. Messages with the same key keep their order. */
final case class Outgoing[T](value: T, key: Option[String] = None, headers: Map[String, String] = Map.empty)
