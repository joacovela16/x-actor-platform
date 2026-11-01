package xplat.di

import cats.effect.Resource

/** Entry point of the declaration DSL, with the effect type fixed so the factories are inferred:
  *
  * {{{
  * val S = Services[IO]
  * S(Agent).dependsOn(Logger, Tools).pure((log, tools) => new Agent(log, tools)) // tools: Many[IO, Tool]
  * }}}
  *
  * Dependencies come before the factory, so Scala infers the parameter types of the lambda.
  */
final class Services[F[_]] private {
  def apply[T](key: Key[T]): Services.Declare0[F, T] = new Services.Declare0[F, T](key)
}

object Services {

  def apply[F[_]]: Services[F] = new Services[F]

  // One class per number of dependencies (up to 8), so each factory receives its dependencies typed.

  /** Declares how the service `key` is built. */
  final class Declare0[F[_], T] private[di] (key: Key[T]) {

    /** The service is a resource: it is released when the container drops it. */
    def resource(make: => Resource[F, T]): Service[F] = Service(key, Nil, _ => make)

    /** The service is the result of an effect, with nothing to release. */
    def eval(make: => F[T]): Service[F] = Service(key, Nil, _ => Resource.eval(make))

    /** The service is a plain value. */
    def pure(make: => T): Service[F] = Service(key, Nil, _ => Resource.pure[F, T](make))

    def dependsOn[D1 <: Key[_]](d1: D1): Declare1[F, T, D1] = new Declare1(key, d1)
    def dependsOn[D1 <: Key[_], D2 <: Key[_]](d1: D1, d2: D2): Declare2[F, T, D1, D2] = new Declare2(key, d1, d2)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_]](d1: D1, d2: D2, d3: D3): Declare3[F, T, D1, D2, D3] = new Declare3(key, d1, d2, d3)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4): Declare4[F, T, D1, D2, D3, D4] = new Declare4(key, d1, d2, d3, d4)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5): Declare5[F, T, D1, D2, D3, D4, D5] = new Declare5(key, d1, d2, d3, d4, d5)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6): Declare6[F, T, D1, D2, D3, D4, D5, D6] = new Declare6(key, d1, d2, d3, d4, d5, d6)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7): Declare7[F, T, D1, D2, D3, D4, D5, D6, D7] = new Declare7(key, d1, d2, d3, d4, d5, d6, d7)
    def dependsOn[D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_], D8 <: Key[_]](d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7, d8: D8): Declare8[F, T, D1, D2, D3, D4, D5, D6, D7, D8] = new Declare8(key, d1, d2, d3, d4, d5, d6, d7, d8)
  }

  final class Declare1[F[_], T, D1 <: Key[_]] private[di] (key: Key[T], d1: D1) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: Nil, make)

    def resource(make: D1#Value[F] => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]]))
    def eval(make: D1#Value[F] => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]])))
    def pure(make: D1#Value[F] => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]])))
  }

  final class Declare2[F[_], T, D1 <: Key[_], D2 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]])))
  }

  final class Declare3[F[_], T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F], D3#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F], D3#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F], D3#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]])))
  }

  final class Declare4[F[_], T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]])))
  }

  final class Declare5[F[_], T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]])))
  }

  final class Declare6[F[_], T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]])))
  }

  final class Declare7[F[_], T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: d7 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F], D7#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]], v(6).asInstanceOf[D7#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F], D7#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]], v(6).asInstanceOf[D7#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F], D7#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]], v(6).asInstanceOf[D7#Value[F]])))
  }

  final class Declare8[F[_], T, D1 <: Key[_], D2 <: Key[_], D3 <: Key[_], D4 <: Key[_], D5 <: Key[_], D6 <: Key[_], D7 <: Key[_], D8 <: Key[_]] private[di] (key: Key[T], d1: D1, d2: D2, d3: D3, d4: D4, d5: D5, d6: D6, d7: D7, d8: D8) {
    private def service(make: List[Any] => Resource[F, T]): Service[F] = Service(key, d1 :: d2 :: d3 :: d4 :: d5 :: d6 :: d7 :: d8 :: Nil, make)

    def resource(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F], D7#Value[F], D8#Value[F]) => Resource[F, T]): Service[F] = service(v => make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]], v(6).asInstanceOf[D7#Value[F]], v(7).asInstanceOf[D8#Value[F]]))
    def eval(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F], D7#Value[F], D8#Value[F]) => F[T]): Service[F] = service(v => Resource.eval(make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]], v(6).asInstanceOf[D7#Value[F]], v(7).asInstanceOf[D8#Value[F]])))
    def pure(make: (D1#Value[F], D2#Value[F], D3#Value[F], D4#Value[F], D5#Value[F], D6#Value[F], D7#Value[F], D8#Value[F]) => T): Service[F] = service(v => Resource.pure[F, T](make(v(0).asInstanceOf[D1#Value[F]], v(1).asInstanceOf[D2#Value[F]], v(2).asInstanceOf[D3#Value[F]], v(3).asInstanceOf[D4#Value[F]], v(4).asInstanceOf[D5#Value[F]], v(5).asInstanceOf[D6#Value[F]], v(6).asInstanceOf[D7#Value[F]], v(7).asInstanceOf[D8#Value[F]])))
  }
}
