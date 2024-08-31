package xplat.extensions.persistence.journal.sqlite

import xplat.lib.expr.core.Ast.Entity
import xplat.lib.expr.core.EType.Clazz
import xplat.lib.expr.core.TAst.Access
import xplat.lib.expr.core.{Ast, AsExpr, DataExtractor, Field, Metadata, Query, Table, EType, TypeMapper, TAst}

import scala.language.implicitConversions

// https://www.sqlitetutorial.net/sqlite-java/update/
// https://github.com/com-lihaoyi/scalasql/blob/main/docs/reference.md
object Main {

  //  final case class Entity(name: String)

  //  object Entity {
  //    implicit def fromString(value: String): Entity = Entity(value)
  //  }

  type Id[T] = T


  //  final case class Query[Q, R](construct: (Q, DataExtractor) => R, adt: ADT) {
  //    def mapADT(f: ADT => ADT): Query[Q, R] = this.modify(_.adt).using(f)
  //  }

  //  Query[String] // .map(x => x * 2)
  //  Query[(TAst[String], TAst[Boolean]), (String, Boolean)] // .map(x => x._2).map(x => x === true)
  //  Query[(TAst[String], TAst[Boolean], TAst[Int]), (String, Boolean, Int)]


  final case class Select[Q, R0](query: Query.Aux[Q, R0]) extends AsExpr[Q] {
    override def toExpr: Q = query.constructFromAST(query.ast)

    def filter(f: Q => TAst[Boolean]): Select[Q, R0] = {

      val localExpr: TAst[Boolean] = f(query.value)

      Select[Q, R0](
        query = new Query[Q] {
          self =>
          override type R = R0

          override def value: Q = query.value

          override def ast: Ast = Ast.Apply("filter", query.ast, localExpr.ast)

          override def getAST(q: Q): Ast = Ast.Apply("filter", query.getAST(q), localExpr.ast)

          override def constructFromAST(ast: Ast): Q = query.constructFromAST(ast)
        }
      )
    }

    def map[Q2](f: Q => Q2)(implicit otherQuery: Query[Q2]): Select[Q2, otherQuery.R] = {

      Select[Q2, otherQuery.R](
        query = new Query[Q2] {
          self =>
          override type R = otherQuery.R

          override def value: Q2 = f(query.value)

          override def constructFromAST(ast: Ast): Q2 = otherQuery.constructFromAST(ast)

          override def getAST(q: Q2): Ast = otherQuery.getAST(q)

          override def ast: Ast = {
            Ast.Apply("map", query.ast, otherQuery.getAST(self.value))
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

          override def ast: Ast = Ast.Apply("flatMap", query.ast, otherQuery.ast)

          override def constructFromAST(ast: Ast): Q1 = otherQuery.constructFromAST(ast)

          override def getAST(q: Q1): Ast = otherQuery.getAST(q)
        }
      )
    }

    def withFilter(f: Q => TAst[Boolean]): Select[Q, R0] = filter(f)

    def join(f: Q => TAst[Boolean]): Select[Q, query.R] = {
      val localExpr = f(query.value)

      Select[Q, query.R](
        query = new Query.MatQuery[Q, query.R] {
          override def value: Q = query.value

          override def ast: Ast = Ast.Apply("join", query.ast, localExpr.ast)

          override def constructFromAST(ast: Ast): Q = query.constructFromAST(ast)

          override def getAST(q: Q): Ast = Ast.Apply("join", query.getAST(q), localExpr.ast)
        }
      )
    }

    def join[G[_[_]]](other: Table[G])(f: (Q, G[TAst]) => TAst[Boolean]): Select[(Q, G[TAst]), (R0, G[Id])] = {

      val g = other.selfContainer.value
      val expr = f(query.value, g)

      Select[(Q, G[TAst]), (R0, G[Id])](
        query = new Query[(Q, G[TAst])] {
          self =>
          override type R = (R0, G[Id])

          override def value: (Q, G[TAst]) = (query.value, g)

          override def ast: Ast = Ast.Apply(
            name = "join",
            over = Ast.Tuple(query.ast, other.selfContainer.ast),
            body = expr.ast
          )

          override def constructFromAST(ast: Ast): (Q, G[TAst]) = (query.constructFromAST(ast), other.selfContainer.constructFromAST(ast))

          override def getAST(q: (Q, G[TAst])): Ast = self.ast
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
      def filter(f: Q1 => TAst[Boolean]):Select[Q1, R1] = ???
      def withFilter(f: Q1 => TAst[Boolean]):Select[Q1, R1] = ???
    }
     */
  }

  final case class City[F[_]](name: F[String])

  object City extends Table[City] {

  }

  final case class Person[F[_]](name: F[String], age: F[Int], city: F[String])

  object Person extends Table[Person]() {
    /*

        override def select: Select[Person[TAst], Person[Id]] = {
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
              new Query[Person[TAst]] {
                self =>
                override type R = Person[Id]
                override def value: Person[TAst] = Person[TAst](
                  name = TAst.Access[String]("name", "person", 0, TypeMapper[String]),
                  age = TAst.Access[Int]("age", "person", 1, TypeMapper[Int]),
                  city = TAst.Access[String]("city", "person", 2, TypeMapper[String])
                )
                override def getAST(q: Person[TAst]): AST = self.ast
                override def ast: AST = AST.Entity(personClazz)
                override def constructFromAST(ast: AST): Person[TAst] = self.value
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

  def main(args: Array[String]): Unit = {

    Person.print()
  }

}
