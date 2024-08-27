package xplat.extensions.persistence.journal.sqlite

import scala.language.implicitConversions

// https://www.sqlitetutorial.net/sqlite-java/update/
// https://github.com/com-lihaoyi/scalasql/blob/main/docs/reference.md
object Main {

  final case class Operation(name: String, literal: String)

  object Operation {

    case object Boolean {
      val OR: Operation = Operation("or", "OR")
      val AND: Operation = Operation("and", "AND")
      val EQUALS: Operation = Operation("equals", "=")
    }

  }

  final case class Entity(name: String)

  object Entity {
    implicit def fromString(value: String): Entity = Entity(value)
  }

  sealed trait Type {}

  final case class Field(name: String, tpe: Type)

  object Type {
    case object Boolean extends Type
    case object String extends Type
    case object Int extends Type
    case object Float extends Type
    case object Byte extends Type
    final case class Clazz(name: String, attributes: List[Field]) extends Type
    def Seq(tpe: Type): Type = Clazz("seq", List(Field("data", tpe)))
    def Tuple(items: Type*): Type = Clazz("tuple", items.zipWithIndex.map { case (d, i) => Field(i.toString, d) }.toList)
  }

  sealed trait AST {}

  object AST {
    case object Empty extends AST
    final case class Entity(tpe: Type.Clazz) extends AST
    final case class FieldAccess(entity: String, field: String, index: Int) extends AST
    final case class BinaryOperation(left: AST, operation: Operation, right: AST) extends AST
    final case class UnaryOperation(adt: AST, operation: Operation) extends AST
    final case class Apply(name: String, over: AST, body: AST = AST.Empty) extends AST
    final case class Tuple(items: AST*) extends AST

    object Apply {
//      def empty(name: String): Function = Function.apply(name, Nil, Map.empty, ADT.Empty)
//      def apply(name: String)(props: (String, ADT)*): Function = Function(name, Nil, props.toMap, ADT.Empty)
//      def apply(name: String, args: ADT*): Function = Function(name, args.toList, Map.empty, ADT.Empty)
    }

    sealed trait Literal extends AST {
      def tpe: Type
    }

    object Literal {
      final case class LInt(data: Int, tpe: Type = Type.Int) extends Literal
      final case class LFloat(data: Float, tpe: Type = Type.Int) extends Literal
      final case class LBoolean(data: Boolean, tpe: Type = Type.Int) extends Literal
      final case class LString(data: String, tpe: Type = Type.Int) extends Literal
      final case class LByteArray(data: Array[Byte], tpe: Type = Type.Int) extends Literal
    }

  }

  sealed trait Expr[T] {
    def ast: AST
    def ===(other: Expr[T]): Expr[Boolean] = Expr.Binary(this.ast, other.ast, Operation.Boolean.EQUALS)
  }

  object Expr {

    final case class Block[T](ast: AST) extends Expr[T]

//    final case class Function[T](name: String, args: List[ADT], props: Map[String, ADT], result: ADT) extends Expr[T] {
//      override def adt: ADT = ADT.Apply(name, args, props, ADT.Empty)
//    }

    final case class Binary[T](left: AST, right: AST, operation: Operation) extends Expr[T] {
      override def ast: AST = AST.BinaryOperation(left, operation, right)
    }

    final case class Unnary[T](expr: Expr[T], operation: Operation) extends Expr[T] {
      override def ast: AST = AST.UnaryOperation(expr.ast, operation)
    }

    final case class Access[T](name: String, entity: String, index: Int, mapper: TypeMapper[T]) extends Expr[T] {
      override def ast: AST = AST.FieldAccess(entity, name, index)
    }

    final case class EString(value: String) extends Expr[String] {
      override def ast: AST = AST.Literal.LString(value)
    }

    final case class EBoolean(value: Boolean, ast: AST) extends Expr[Boolean]

    final case class EInt(value: Int) extends Expr[Int] {
      override def ast: AST = AST.Literal.LInt(value)
    }

    final case class Wrapper[T](ast: AST) extends Expr[T] {}

    object EBoolean {
      def apply(value: Boolean): EBoolean = EBoolean(value, AST.Literal.LBoolean(value))
    }

    implicit def mkString(str: String): Expr[String] = EString(str)
    implicit def mkInt(value: Int): Expr[Int] = EInt(value)
    implicit def mkBoolean(value: Boolean): Expr[Boolean] = EBoolean(value)

    implicit class BooleanPredef(left: Expr[Boolean]) {
      def &&(right: Expr[Boolean]): Expr[Boolean] = Expr.Binary(left.ast, right.ast, Operation.Boolean.AND)
      def ||(right: Expr[Boolean]): Expr[Boolean] = Expr.Binary(left.ast, right.ast, Operation.Boolean.OR)
    }

    implicit class StringPredef(left: Expr[String]) {
      def *(right: Expr[Int]): Expr[String] = ???
    }

  }

  type Id[T] = T

  trait DataExtractor {}
  trait DataStore {}

  trait TypeMapper[T] {
    def tpe: Type
    def get(dataExtractor: DataExtractor, index: Int): Either[String, T]
    def set(dataStore: DataStore, index: Int): Either[String, DataStore]
  }

  object TypeMapper {
    def apply[T](implicit typeMapper: TypeMapper[T]): typeMapper.type = typeMapper

    implicit val string: TypeMapper[String] = new TypeMapper[String] {
      override def tpe: Type = Type.String

      override def get(dataExtractor: DataExtractor, index: Int): Either[String, String] = ???

      override def set(dataStore: DataStore, index: Int): Either[String, DataStore] = ???
    }

    implicit val int: TypeMapper[Int] = new TypeMapper[Int] {
      override def tpe: Type = Type.Int
      override def get(dataExtractor: DataExtractor, index: Int): Either[String, Int] = ???

      override def set(dataStore: DataStore, index: Int): Either[String, DataStore] = ???
    }

    implicit val boolean: TypeMapper[Boolean] = new TypeMapper[Boolean] {
      override def tpe: Type = Type.Boolean
      override def get(dataExtractor: DataExtractor, index: Int): Either[String, Boolean] = ???

      override def set(dataStore: DataStore, index: Int): Either[String, DataStore] = ???
    }

    implicit def t2[R1, R2](implicit r1: TypeMapper[R1], r2: TypeMapper[R2]): TypeMapper[(R1, R2)] = ???
    implicit def t3[R1, R2, R3](implicit r1: TypeMapper[R1], r2: TypeMapper[R2], r3: TypeMapper[R3]): TypeMapper[(R1, R2, R3)] = ???
  }

//  final case class Query[Q, R](construct: (Q, DataExtractor) => R, adt: ADT) {
//    def mapADT(f: ADT => ADT): Query[Q, R] = this.modify(_.adt).using(f)
//  }

  trait Query[Q] {
    type R
    def value: Q
    def construct(q: Q, de: DataExtractor): R = ???
    def ast: AST
    def constructFromAST(ast: AST): Q
    def getAST(q: Q): AST
  }

  object Query {
    type Aux[Q, R0] = Query[Q]{type R = R0}

    trait MatQuery[Q, R0] extends Query[Q]{
      override type R = R0
    }

    implicit def fromExpr[R0](implicit tm: TypeMapper[R0]): Query.Aux[Expr[R0], R0] = new Query[Expr[R0]] {
      override type R = R0
      override def value: Expr[R] = Expr.Wrapper(AST.Empty)
      //  override def transform(q: Expr[R]): Expr[R] = ???
//      override def extractAST(q: Expr[R]): AST = q.ast
      override def ast: AST = AST.Empty

      override def constructFromAST(ast: AST): Expr[R] = Expr.Wrapper(ast)

      override def getAST(q: Expr[R]): AST = q.ast
    }

//    def apply[Q, R](implicit q: Query[Q, R]): q.type = q
//    def apply[R](implicit q: Query[Expr[R], R]): q.type = q

    implicit def qt2[Q1, Q2](implicit q1: Query[Q1], q2: Query[Q2]): Query.Aux[(Q1, Q2), (q1.R, q2.R)] = {

      new Query[(Q1, Q2)] {
        override type R = (q1.R, q2.R)

        override def value: (Q1, Q2) = (q1.value, q2.value)

        override def ast: AST = AST.Tuple(q1.ast, q2.ast)

        override def getAST(q: (Q1, Q2)): AST = AST.Tuple(q1.getAST(q._1), q2.getAST(q._2))

        override def constructFromAST(ast: AST): (Q1, Q2) = {
          (q1.constructFromAST(ast), q2.constructFromAST(ast))
        }
      }
    }

  }

//  Query[String] // .map(x => x * 2)
//  Query[(Expr[String], Expr[Boolean]), (String, Boolean)] // .map(x => x._2).map(x => x === true)
//  Query[(Expr[String], Expr[Boolean], Expr[Int]), (String, Boolean, Int)]

  trait AsExpr[T] {
    def toExpr: T
  }

  trait AsADT[Q] {
    def adt(q: Q): AST
  }

  final case class Select[Q, R0](
      query: Query.Aux[Q, R0]
  ) extends AsExpr[Q] {
    override def toExpr: Q = query.constructFromAST(query.ast)

    def filter(f: Q => Expr[Boolean]): Select[Q, R0] = {

      val localExpr: Expr[Boolean] = f(query.value)

      Select[Q, R0](
        query = new Query[Q] { self =>
          override type R = R0
          override def value: Q = query.value
          override def ast: AST = AST.Apply("filter", query.ast, localExpr.ast)
          override def getAST(q: Q): AST = AST.Apply("filter", query.getAST(q), localExpr.ast)
          override def constructFromAST(ast: AST): Q = query.constructFromAST(ast)
        }
      )
    }

    def map[Q2](f: Q => Q2)(implicit otherQuery: Query[Q2]): Select[Q2, otherQuery.R] = {

      Select[Q2, otherQuery.R](
        query = new Query[Q2] { self =>
          override type R = otherQuery.R
          override def value: Q2 = f(query.value)
          override def constructFromAST(ast: AST): Q2 = otherQuery.constructFromAST(ast)
          override def getAST(q: Q2): AST = otherQuery.getAST(q)
          override def ast: AST = {
            AST.Apply("map", query.ast, otherQuery.getAST(self.value))
          }
        }
      )

    }

    def flatMap[Q1, R1](f: Q => Select[Q1, R1]): Select[Q1, R1] = {
      val localValue = f(query.value)
      val otherQuery = localValue.query

      Select[Q1, R1](
        query = new Query[Q1] {
          override type R = R1
          override def value: Q1 = otherQuery.value

          override def ast: AST = AST.Apply("flatMap", query.ast, otherQuery.ast)

          override def constructFromAST(ast: AST): Q1 = otherQuery.constructFromAST(ast)

          override def getAST(q: Q1): AST = otherQuery.getAST(q)
        }
      )
    }

    def withFilter(f: Q => Expr[Boolean]): Select[Q, R0] = filter(f)

    def join(f: Q => Expr[Boolean]): Select[Q, query.R] = {
      val localExpr = f(query.value)

      Select[Q, query.R](
        query = new Query.MatQuery[Q, query.R] {
          override def value: Q = query.value
          override def ast: AST = AST.Apply("join", query.ast , localExpr.ast)
          override def constructFromAST(ast: AST): Q = query.constructFromAST(ast)
          override def getAST(q: Q): AST = AST.Apply("join", query.getAST(q), localExpr.ast)
        }
      )
    }

    def join[G[_[_]]](other: Table[G])(f: (Q, G[Expr]) => Expr[Boolean]): Select[(Q, G[Expr]), (R0, G[Id])] = {

      val g = other.selfContainer.value
      val expr = f(query.value, g)

      Select[(Q, G[Expr]), (R0, G[Id])](
        query = new Query[(Q, G[Expr])] {
          self =>
          override type R = (R0, G[Id])
          override def value: (Q, G[Expr]) = (query.value, g)

          override def ast: AST = AST.Apply(
            name = "join",
            over = AST.Tuple(query.ast, other.selfContainer.ast),
            body = expr.ast
          )

          override def constructFromAST(ast: AST): (Q, G[Expr]) = (query.constructFromAST(ast), other.selfContainer.constructFromAST(ast))

          override def getAST(q: (Q, G[Expr])): AST = self.ast
        }
      )
    }
//    def toExpr: Q

  }

  object Select {

    /*final case class Mapper[Q1, R1, Q2, R2](
    ) {

      def map(f: Q1 => Q2)(implicit q:Query[Q2, R2]): Select[Q2, R2] = ???
      def flatMap(f: Q1 => Select[Q2, R2])(implicit q:Query[Q2, R2]):Select[Q2, R2] = ???
      def filter(f: Q1 => Expr[Boolean]):Select[Q1, R1] = ???
      def withFilter(f: Q1 => Expr[Boolean]):Select[Q1, R1] = ???
    }
*/
  }

  trait Metadata[M[_[_]]] {
    def query: Query.Aux[M[Expr], M[Id]]
  }

  abstract class Table[M[_[_]]] {

    implicit def selfContainer: Query.Aux[M[Expr], M[Id]] = select.query

    def select: Select[M[Expr], M[Id]]

    def join(f: M[Expr] => Expr[Boolean]) = select.join(f)

  }

  final case class City[F[_]](name: F[String])

  final case class Person[F[_]](name: F[String], age: F[Int], city: F[String])

  object Person extends Table[Person] {

    override def select: Select[Person[Expr], Person[Id]] = {
      val personClazz: Type.Clazz = Type.Clazz(
        "person",
        List(
          Field("name", Type.String),
          Field("age", Type.Int),
          Field("city", Type.String)
        )
      )

      Select(
        query = {
          new Query[Person[Expr]] {
            self =>
            override type R = Person[Id]
            override def value: Person[Expr] = Person[Expr](
              name = Expr.Access[String]("name", "person", 0, TypeMapper[String]),
              age = Expr.Access[Int]("age", "person", 1, TypeMapper[Int]),
              city = Expr.Access[String]("city", "person", 2, TypeMapper[String])
            )
            override def getAST(q: Person[Expr]): AST = self.ast
            override def ast: AST = AST.Entity(personClazz)
            override def constructFromAST(ast: AST): Person[Expr] = self.value
          }
        }
      )
    }

  }

  val x = Person.select
//    .filter(x => x.name === "pepe")
    //.filter(x => x.name === Person.select.filter(x => x.age === 3).map(_.city).toExpr)
    //   .map(x => x.age)
    .map(x => (x.name, x))
    .map(x => x._2)
//    .join(Person) { (p, c) => p.name === c.city }
  //    .flatMap(_ => Person.select)
//    .map(x => x._2)

  val y =   for {
    s1 <- Person.select
    s2 <- Person.join(x=>x.name === "pepe") if s2.age === 3
  } yield s1.name

  /*

  Person.select
    .flatMap(s1 =>
      Person.join(x => x.name === "pepe")
        .withFilter(s2 => s2.age === 3)
        .map(s2 => s1.name)
    )
   */

  pprint.pprintln(x.query.ast)
  /*.join(City) { (p, c) => p.name === c.name }
    .map(_._1)
    .map(x => x.name)
    .filter { name =>
      City.select.map(x => x.name).toExpr === name
    }
    .toExpr*/

  def main(args: Array[String]): Unit = {}

}
