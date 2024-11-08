package xplat.lib.optimizer

sealed trait AMPLCommand[+T]

object AMPLCommand {

  final case class DeclareInputSet[T](name: String, set: SSet[T]) extends AMPLCommand[SSet[T]]
  final case class DeclareSet[T](name: String, set: SSet[T]) extends AMPLCommand[SSet[T]]

  final case class DeclareInputParam[T, Domain](name: String) extends AMPLCommand[Param[T, Domain]]
  final case class DeclareParam[T, Domain](name: String) extends AMPLCommand[Param[T, Domain]]

  final case class DeclareVariable[T, Domain](name: String)
  final case class DeclareConstraint[T, Domain](name: String)
  final case class DeclareObjective[T, Domain](name: String)
  case object Solve extends AMPLCommand[Nothing]
  final case class Display()
}
