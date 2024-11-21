package xplat.lib.optimizer.model.dsl

final case class Arg[T](index: Int, referenceTo: String) {
  def asRef: Arg.Ref = Arg.Ref(index, referenceTo)
}

object Arg {
  final case class Ref(index: Int, referenceTo: String)
}
