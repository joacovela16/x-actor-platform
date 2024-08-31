package xplat.lib.expr.core

trait Client[F[_]] {
  def run[Q](qp: QueryProvider[Q]): F[LazyList[qp.R]]
  def stream[Q](qp: QueryProvider[Q]): fs2.Stream[F, qp.R]
}
