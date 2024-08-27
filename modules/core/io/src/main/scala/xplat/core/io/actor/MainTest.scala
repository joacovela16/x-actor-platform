package xplat.core.io.actor
//import scalasql._
//import scalasql. SqliteDialect._

object MainTest {
  // Define your table model classes
  case class City[T[_]](
                         id: T[Int],
                         name: T[String],
                         countryCode: T[String],
                         district: T[String],
                         population: T[Long]
                       )
//  object City extends Table[City]

//  City.select.map(x=> x.id === 3 && x.name === "")
}
