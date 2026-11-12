package xplat.di

import cats.effect.Resource

/** A service declared as data: which key it provides, what it needs and how it is built. Declare it with [[Key]]
  * and register it with [[Container.provide]].
  */
final class Service[F[_]] private[di] (val key: Key[_], val deps: List[Key[_]], val make: List[Any] => Resource[F, Any]) {
  override def toString: String = s"Service(${key.name} <- ${deps.map(_.name).mkString(", ")})"
}

object Service {
  private[di] def apply[F[_], T](key: Key[T], deps: List[Key[_]], make: List[Any] => Resource[F, T]): Service[F] =
    new Service[F](key, deps, make.asInstanceOf[List[Any] => Resource[F, Any]])
}
