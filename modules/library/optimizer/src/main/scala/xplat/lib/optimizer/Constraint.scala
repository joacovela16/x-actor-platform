package xplat.lib.optimizer

final case class Constraint( body: Expr[Boolean]){
  def describe(str:String):Constraint = ???
}
