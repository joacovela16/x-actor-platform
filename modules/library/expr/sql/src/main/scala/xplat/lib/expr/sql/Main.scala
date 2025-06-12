package xplat.lib.expr.sql

import xplat.lib.expr.core.Table
import xplat.lib.expr.sql.sqlite.SQLiteDialect

object Main extends SQLiteDialect {

  final case class City[F[_]](name: F[String], amount: F[Int])
  object City extends Table[City] {}

  final case class Person[F[_]](name: F[String], age: F[Int], city: F[String])
  object Person extends Table[Person]()

  def main(args: Array[String]): Unit = {

    val q1 = Person.select
      .filter(x => x.name === "pepe")

    val q2 = Person.select
      .flatMap { s1 =>
        Person.select
          .map(x => x.age)
          .withFilter(s2 => s1.age === s2)
          .flatMap { s2 =>
            City.select
              .join(x => s2 === x.amount)
              .map(s3 => s3)
          }
      }

    pprint.pprintln(
       Person.select.join(City)((a, b) => a.city === b.name).ast
      //q2.ast
    )

//    pprint.pprintln(q1.ast)

//    pprint.pprintln(q2.ast)

    val i1 = Person.insert.create(_.age := 23, _.name := "Joaquin")
    val i2 = Person.insert.batch(_(_.age := 23, _.name := "Joaquin"), _(_.age := 23, _.name := "Joaquin"))

//    pprint.pprintln(i2.ast)

  }

}
