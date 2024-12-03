package xplat.lib.optimizer.model.dsl

sealed trait VariableCriteria

object VariableCriteria {

  case object Unrestricted extends VariableCriteria
  final case class DomainBy(domain: Domain) extends VariableCriteria
  final case class Bound(value: Double, relationType: RelationType) extends VariableCriteria

  sealed trait Domain {}

  object Domain {
    case object Double extends Domain
    case object Integer extends Domain
  }

}
