package xplat.lib.optimizer.model.dsl

sealed trait SSet[T] {
  def id: String
  def asDecl:SetDecl
}

object SSet {
  final case class Open[T](id: String) extends SSet[T] {
    override def asDecl: SetDecl = SetDecl.Dynamic(id)
  }

  final case class Literal[T](id: String, values: Seq[T], codec: Datum.Codec[T]) extends SSet[T] {
    override def asDecl: SetDecl = SetDecl.Literal(id = id, values = values.map(codec.asDatum))
  }

  final case class Range(id: String, from: Int, to: Int) extends SSet[Int]{
    override def asDecl: SetDecl = SetDecl.Range(id, math.min(from, to), math.max(from, to))
  }
}
