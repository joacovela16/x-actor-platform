package xplat.lib.expr.core


trait AsExpr[T] {
  def toExpr: T
}