package xplat.di

/** Identifies a service and its type. Keys are compared by name, so the same key declared in two places (e.g. a plugin
  * that bundles its own copy) matches: names must be unique.
  */
sealed abstract class Key[T](val name: String) {

  /** What a service that depends on this key receives. */
  type Value[F[_]]

  def isMultiple: Boolean

  override def toString: String = s"Key($name)"
  override def equals(other: Any): Boolean = other match {
    case key: Key[_] => key.name == name
    case _           => false
  }
  override def hashCode: Int = name.hashCode
}

object Key {

  /** A key with exactly one service. Dependents receive the service itself. */
  final class Single[T] private[di] (name: String) extends Key[T](name) {
    type Value[F[_]] = T
    def isMultiple: Boolean = false
  }

  /** A key with any number of services, in registration order. Dependents receive a live [[Many]]. */
  final class Multiple[T] private[di] (name: String) extends Key[T](name) {
    type Value[F[_]] = Many[F, T]
    def isMultiple: Boolean = true
  }

  def single[T](name: String): Single[T] = new Single[T](name)
  def multiple[T](name: String): Multiple[T] = new Multiple[T](name)
}
