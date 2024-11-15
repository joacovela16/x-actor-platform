package xplat.lib.optimizer.model

final case class Constraint( body: Expr.Logical){
  def describe(str:String):Constraint = ???
}
