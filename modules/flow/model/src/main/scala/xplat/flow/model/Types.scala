package xplat.flow.model

import io.circe.Json

/** What the types of a flow know about the entities of the business model ([[DataType.Entity]], [[DataType.Ref]]).
  *
  * The flow model does not know how an entity is defined: whoever keeps the definitions (the entity module) answers these
  * questions, the same way a [[FlowResolver]] finds the flows that a flow calls.
  */
trait Types {

  /** The fields of the entity, with their types; `None` if there is no entity with that name. */
  def fields(entity: String): Option[Map[String, DataType]]

  /** The type of the key of an entity with identity, which is the type of a reference to it. */
  def key(entity: String): Option[DataType]

  /** `value` as the entity makes it (its defaults applied and its computed fields filled), or every rule it breaks. */
  def conform(entity: String, value: Json): Either[List[DataType.Problem], Json]
}

object Types {

  /** Knows no entity: a value of an entity type is never accepted. */
  val empty: Types = new Types {
    def fields(entity: String): Option[Map[String, DataType]] = None
    def key(entity: String): Option[DataType] = None
    def conform(entity: String, value: Json): Either[List[DataType.Problem], Json] =
      Left(List(DataType.Problem(Nil, "unknown-entity", s"the entity '$entity' does not exist")))
  }
}
