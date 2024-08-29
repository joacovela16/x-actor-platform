package xplat.extensions.persistence.journal.sqlite

import xplat.lib.expr.core.{AST, AsExpr, DataExtractor, TypedAst, Field, Query, Table, Type, TypeMapper}

import scala.language.implicitConversions

// https://www.sqlitetutorial.net/sqlite-java/update/
// https://github.com/com-lihaoyi/scalasql/blob/main/docs/reference.md
object Main {

  final case class Entity(name: String)

  object Entity {
    implicit def fromString(value: String): Entity = Entity(value)
  }

  type Id[T] = T


//  final case class Query[Q, R](construct: (Q, DataExtractor) => R, adt: ADT) {
//    def mapADT(f: ADT => ADT): Query[Q, R] = this.modify(_.adt).using(f)
//  }

//  Query[String] // .map(x => x * 2)
//  Query[(TypedAst[String], TypedAst[Boolean]), (String, Boolean)] // .map(x => x._2).map(x => x === true)
//  Query[(TypedAst[String], TypedAst[Boolean], TypedAst[Int]), (String, Boolean, Int)]


  final case class Select[Q, R0](query: Query.Aux[Q, R0]) extends AsExpr[Q] {
    override def toExpr: Q = query.constructFromAST(query.ast)

    def filter(f: Q => TypedAst[Boolean]): Select[Q, R0] = {

      val localExpr: TypedAst[Boolean] = f(query.value)

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

    def withFilter(f: Q => TypedAst[Boolean]): Select[Q, R0] = filter(f)

    def join(f: Q => TypedAst[Boolean]): Select[Q, query.R] = {
      val localExpr = f(query.value)

      Select[Q, query.R](
        query = new Query.MatQuery[Q, query.R] {
          override def value: Q = query.value
          override def ast: AST = AST.Apply("join", query.ast, localExpr.ast)
          override def constructFromAST(ast: AST): Q = query.constructFromAST(ast)
          override def getAST(q: Q): AST = AST.Apply("join", query.getAST(q), localExpr.ast)
        }
      )
    }

    def join[G[_[_]]](other: Table[G])(f: (Q, G[TypedAst]) => TypedAst[Boolean]): Select[(Q, G[TypedAst]), (R0, G[Id])] = {

      val g = other.selfContainer.value
      val expr = f(query.value, g)

      Select[(Q, G[TypedAst]), (R0, G[Id])](
        query = new Query[(Q, G[TypedAst])] {
          self =>
          override type R = (R0, G[Id])
          override def value: (Q, G[TypedAst]) = (query.value, g)

          override def ast: AST = AST.Apply(
            name = "join",
            over = AST.Tuple(query.ast, other.selfContainer.ast),
            body = expr.ast
          )

          override def constructFromAST(ast: AST): (Q, G[TypedAst]) = (query.constructFromAST(ast), other.selfContainer.constructFromAST(ast))

          override def getAST(q: (Q, G[TypedAst])): AST = self.ast
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
      def filter(f: Q1 => TypedAst[Boolean]):Select[Q1, R1] = ???
      def withFilter(f: Q1 => TypedAst[Boolean]):Select[Q1, R1] = ???
    }
     */
  }

  final case class City[F[_]](name: F[String])

  final case class Person[F[_]](name: F[String], age: F[Int], city: F[String])

  object Person extends Table[Person] {
/*

    override def select: Select[Person[TypedAst], Person[Id]] = {
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
          new Query[Person[TypedAst]] {
            self =>
            override type R = Person[Id]
            override def value: Person[TypedAst] = Person[TypedAst](
              name = TypedAst.Access[String]("name", "person", 0, TypeMapper[String]),
              age = TypedAst.Access[Int]("age", "person", 1, TypeMapper[Int]),
              city = TypedAst.Access[String]("city", "person", 2, TypeMapper[String])
            )
            override def getAST(q: Person[TypedAst]): AST = self.ast
            override def ast: AST = AST.Entity(personClazz)
            override def constructFromAST(ast: AST): Person[TypedAst] = self.value
          }
        }
      )
    }
*/

  }

//  val x = Person.select
//    .filter(x => x.name === "pepe")
    // .filter(x => x.name === Person.select.filter(x => x.age === 3).map(_.city).toExpr)
    //   .map(x => x.age)
//    .map(x => (x.name, x))
//    .map(x => x._2)
//    .join(Person) { (p, c) => p.name === c.city }
  //    .flatMap(_ => Person.select)
//    .map(x => x._2)

  /*val y = for {
    s1 <- Person.select
    s2 <- Person.join(x => x.name === "pepe") if s2.age === 3
  } yield s1.name*/

  /*

  Person.select
    .flatMap(s1 =>
      Person.join(x => x.name === "pepe")
        .withFilter(s2 => s2.age === 3)
        .map(s2 => s1.name)
    )
   */

  // pprint.pprintln(x.query.ast)
  /*.join(City) { (p, c) => p.name === c.name }
    .map(_._1)
    .map(x => x.name)
    .filter { name =>
      City.select.map(x => x.name).toExpr === name
    }
    .toExpr*/

  def main(args: Array[String]): Unit = {}

}
