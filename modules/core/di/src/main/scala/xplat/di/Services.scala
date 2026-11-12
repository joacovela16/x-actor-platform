package xplat.di

import cats.effect.Resource

import scala.language.implicitConversions

/** The declaration DSL behind [[Key]]'s `declare` conversion: a key declares how its service is built.
  *
  * {{{
  * import xplat.di.Key.declare
  * Agent.dependsOn(Logger, Tools).pure((log, tools) => new Agent(log, tools)) // tools: Many[IO, Tool]
  * }}}
  *
  * The effect type is not written anywhere: Scala infers it from the expected type (`Container#provide`, or a
  * declared `List[Service[IO]]`). Dependencies come before the factory, so Scala infers the parameter types of the
  * lambda.
  */
object Services {

  // One class per number of dependencies (up to 8), so each factory receives its dependencies typed.

  /** Declares how the service `key` is built. */
  final class Declare0[T] private[di] (key: Key[T]) {

    /** The service is a resource: it is released when the container drops it. */
    def resource[F[_]](make: => Resource[F, T]): Service[F] = Service(key, Nil, _ => make)

    /** The service is the result of an effect, with nothing to release. */
    def eval[F[_]](make: => F[T]): Service[F] = Service(key, Nil, _ => Resource.eval(make))

    /** The service is a plain value. */
    def pure[F[_]](make: => T): Service[F] = Service(key, Nil, _ => Resource.pure[F, T](make))

    def dependsOn[D1 <: Key[_]](d1: D1): Declare1[T, D1] = new Declare1(key, d1)
    def dependsOn[D1 <: Key[_], D2 <: Key[_]](d1: D1, d2: D2): Declare2[T, D1, D2] = new Declare2(key, d1, d2)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_]](d1: D1, d2: D2, d3: D3): Declare3[T, D1, D2, D3] = new Declare3(key, d1, d2, d3)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4): Declare4[T, D1, D2, D3, D4] = new Declare4(key, d1, d2, d3, d4)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5): Declare5[T, D1, D2, D3, D4, D5] = new Declare5(key, d1, d2, d3, d4, d5)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6): Declare6[T, D1, D2, D3, D4, D5, D6] = new Declare6(key, d1, d2, d3, d4, d5, d6)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7): Declare7[T, D1, D2, D3, D4, D5, D6, D7] = new Declare7(key, d1, d2, d3, d4, d5, d6, d7)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_], D8 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7, d8: D8): Declare8[T, D1, D2, D3, D4, D5, D6, D7, D8] = new Declare8(key, d1, d2, d3, d4, d5, d6, d7, d8)
  }

  final class Declare1[T, D1 <: Key[_]] private[di] (key: Key[T], d1: D1) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: Nil, make)

    def resource[F[_]](make: Dep.Of[D1, F] => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]]))
    def eval[F[_]](make: Dep.Of[D1, F] => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]])))
    def pure[F[_]](make: Dep.Of[D1, F] => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]])))
  }

  final class Declare2[T, D1 <: Key[_], D2 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]])))
  }

  final class Declare3[T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]])))
  }

  final class Declare4[T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]])))
  }

  final class Declare5[T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]])))
  }

  final class Declare6[T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]])))
  }

  final class Declare7[T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: d7 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F], Dep.Of[D7, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]], v(6).asInstanceOf[Dep.Of[D7, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F], Dep.Of[D7, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]], v(6).asInstanceOf[Dep.Of[D7, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F], Dep.Of[D7, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]], v(6).asInstanceOf[Dep.Of[D7, F]])))
  }

  final class Declare8[T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_], D8 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7, d8: D8) {
    private def service[F[_]](make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: d7 :: d8 :: Nil, make)

    def resource[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F], Dep.Of[D7, F], Dep.Of[D8, F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]], v(6).asInstanceOf[Dep.Of[D7, F]], v(7).asInstanceOf[Dep.Of[D8, F]]))
    def eval[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F], Dep.Of[D7, F], Dep.Of[D8, F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]], v(6).asInstanceOf[Dep.Of[D7, F]], v(7).asInstanceOf[Dep.Of[D8, F]])))
    def pure[F[_]](make: (Dep.Of[D1, F], Dep.Of[D2, F], Dep.Of[D3, F], Dep.Of[D4, F], Dep.Of[D5, F], Dep.Of[D6, F], Dep.Of[D7, F], Dep.Of[D8, F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[Dep.Of[D1, F]], v(1).asInstanceOf[Dep.Of[D2, F]], v(2).asInstanceOf[Dep.Of[D3, F]], v(3).asInstanceOf[Dep.Of[D4, F]], v(4).asInstanceOf[Dep.Of[D5, F]], v(5).asInstanceOf[Dep.Of[D6, F]], v(6).asInstanceOf[Dep.Of[D7, F]], v(7).asInstanceOf[Dep.Of[D8, F]])))
  }
}
