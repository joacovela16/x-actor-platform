package xplat.lib.optimizer.model

sealed trait SetDecl {
  def id: String
}

object SetDecl {
  final case class Dynamic(id: String) extends SetDecl
  final case class Literal(id: String, values: Seq[Datum]) extends SetDecl
  final case class Range(id: String, from: Int, to: Int) extends SetDecl
}
