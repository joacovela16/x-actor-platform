package xplat.core.connectors

/** A message read from a broker.
  *
  * @param source
  *   the topic, queue or stream it came from.
  * @param key
  *   messages with the same key keep their order (Kafka partition key, JMS message group, ...).
  * @param position
  *   where it is in the source, for logs (e.g. `partition@offset`, a message id).
  */
final case class Incoming[T](source: String, key: Option[String], value: T, headers: Map[String, String], position: String)
