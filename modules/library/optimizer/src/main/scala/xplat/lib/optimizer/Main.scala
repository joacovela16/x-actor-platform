package xplat.lib.optimizer

object Main {

  /*
  val P = solver.set[Int]
  val M = solver.union(solver.set.of(0), P)
  val p1 = solver.param[Int].withDomain(P, M).where((p,m) => )
  val p2 = solver.param[Int].withDomain(P, M).where((p,m) => )
  val x = solver.var[Int]()
  val y = solver.var(domain = solver.domain(...), description = "...")
  solver
    .minimize(
      solver.sum()
    )(
      solver.st()
    )
   */

  case object Solver {

    sealed trait ApplyBuilder[In, Out] {
      type T
      def apply(t: T): In => Out
    }

    object ApplyBuilder {
      type Aux[In, Out, T0] = ApplyBuilder[In, Out] { type T = T0 }

      implicit def unit[Out]: ApplyBuilder.Aux[Unit, Out, () => Out] = ???
      implicit def a2[I0, I1, Out]: ApplyBuilder.Aux[(I0, I1), Out, (I0, I1) => Out] = ???
    }

    final case class DomainContainer[T <: Product]() {
      def where[A](apply: A)(implicit a: ApplyBuilder.Aux[T, Int, A]): DomainContainer[T] = ???
    }

    object DomainContainer {
      val Empty: DomainContainer[Product1[Unit]] = ???
      def apply[T <: Product](sets: T): DomainContainer[T] = ???
    }

    trait DomainContainerLP {}

    sealed trait DomainRestriction[T] {}

    object DomainRestriction {
      final case class Default[T](value: T) extends DomainRestriction[T]
      final case class Relation[T](value: T, relationType: RelationType) extends DomainRestriction[T]
    }

    sealed trait RelationType {}

    object RelationType {
      case object GreatThan extends RelationType
      case object GreatOrEqualsThan extends RelationType
      case object LessOrEqualsThan extends RelationType
      case object LessThan extends RelationType
    }

    sealed trait AsTuple[A] {
      type T <: Product
    }

    trait AsTupleLP {

      implicit def single[T]: AsTuple.Aux[T, Product1[T]] = ???
    }

    object AsTuple extends AsTupleLP {
      type Aux[A, T0] = AsTuple[A] { type T = T0 }
      implicit def default[T <: Product]: AsTuple.Aux[T, T] = ???
    }

    sealed trait ObjectiveType {}

    object ObjectiveType {
      case object Minimize extends ObjectiveType
      case object Maximize extends ObjectiveType
    }

    final case class LeftExprContainer()
    final case class FullExprContainer()

    sealed trait Range {}

    object Range {
      final case class Lower(value: Double) extends Range
      final case class Upper(value: Double) extends Range
      final case class Bounded(from: Double, to: Double) extends Range
    }

    sealed trait Variable {}

    object Variable {
      final case class Boolean(name: String) extends Variable
      final case class Integer(name: String, range: Range) extends Variable
      final case class Double(name: String, range: Range) extends Variable
    }

    sealed trait Primitive {}

    object Primitive {}

  }

  final case class ElseDecl[T]() {
    def Else(condBody: Expr[T]): Expr[T] = ???
    def ElseIf(cond: Expr[Boolean])(condBody: Expr[T]): ElseDecl[T] = ???
  }

  def If[T](cond: Expr[Boolean])(thenBody: Expr[T]): ElseDecl[T] = ???

  def set[T](implicit k: Typed[T, Kind], sc: sourcecode.Name): SSet[T] = SSet.Open(sc.value)
  def set[T](head: T, tail: T*)(implicit sc: sourcecode.Name): SSet[T] = SSet.Literal(sc.value, head +: tail)
  def union[T](head: SSet[T], tail: SSet[T]*)(implicit sc: sourcecode.Name): SSet[T] = SSet.Union(sc.value, head +: tail)
  def set(from: Int, to: Int)(implicit sc: sourcecode.Name): SSet[Int] = SSet.Range(sc.value, from, to)

  def param[T](implicit k: Typed[T, Kind]): Param[T, Unit] = ???
  def variable[T](implicit k: IsValidVariable[T]): Variable[T, Unit] = variable()
  def variable[T](lowerBound: Option[T] = None, upperBound: Option[T] = None)(implicit k: IsValidVariable[T]): Variable[T, Unit] = ???

  def sum[D](value: D): SumExpr[D] = ???

  def minimize[T](expr: Expr[T])(implicit c: IsValidVariable[T]): Model = ???

  def subjectTo(expr: Expr[Boolean]): Constraint = ???
  def st(expr: Expr[Boolean]): Constraint = subjectTo(expr)
  def st[D](value: D): SubjectTo[D] = ???

  final case class SubjectTo[D]() {}

  object SubjectTo {

    implicit class Predef[D, Tuple, TupleExpr, Apply, Conditional](subjectTo: SubjectTo[D])(implicit ops: SetContainerOps.Aux[D, Boolean, Tuple, TupleExpr, Apply, Conditional]) {
      def apply(x: Conditional)(expr: Apply): Constraint = ???
    }

  }

  val S1 = set[Int]
  val S2 = set[Boolean]
  val S3 = set[Int]
  val p1 = param[Int].domain(S1, S3)

  sum(S1)(b => b < 10)(b => b + 3)

  val p3 = param[Boolean]
    .domain(S1)
    .project { i =>
      If(i === 0) {
        true
      } Else {
        false
      }
    }

  val p2 = param[Int].domain(S1, S2).project { (a, b) =>
    If(p1(1, 2) === a && b) {
      1
    } Else {
      0
    }
  }

  val v1 = variable[Int].domain(S1, S2)
  val v2 = variable[Int].domain(S1).where(x => p3(x))
  val x = variable[Double]
  val y = variable[Double]

  minimize(x + y).subjectTo(
    st(x > 8),
    st(y < 20),
    st(x - y > 10),
    st(S1, S2)((i, k) => i < k)((i, k) => v1(i, k) < 9)
      describe "pepep"
  )

}
