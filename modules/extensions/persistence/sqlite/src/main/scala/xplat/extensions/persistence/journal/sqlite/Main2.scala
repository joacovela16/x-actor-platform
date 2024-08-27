package xplat.extensions.persistence.journal.sqlite

import scalasql.Table
import scalasql.SqliteDialect._

object Main2 {
  case class Country[T[_]](
                            code: T[String],
                            name: T[String],
                            continent: T[String],
                            region: T[String],
                            surfaceArea: T[Int],
                            indepYear: T[Option[Int]],
                            population: T[Long],
                            lifeExpectancy: T[Option[Double]],
                            gnp: T[Option[scala.math.BigDecimal]],
                            gnpOld: T[Option[scala.math.BigDecimal]],
                            localName: T[String],
                            governmentForm: T[String],
                            headOfState: T[Option[String]],
                            capital: T[Option[Int]],
                            code2: T[String]
                          )

  object Country extends Table[Country]()

  case class City[T[_]](
                         id: T[Int],
                         name: T[String],
                         countryCode: T[String],
                         district: T[String],
                         population: T[Long]
                       )

  object City extends Table[City]()

  case class CountryLanguage[T[_]](
                                    countryCode: T[String],
                                    language: T[String],
                                    isOfficial: T[Boolean],
                                    percentage: T[Double]
                                  )

  object CountryLanguage extends Table[CountryLanguage]()

  for {
    s1 <- Country.select
    s2 <- CountryLanguage.join(x=>x.language === s1.name)
    if s2.isOfficial === true
  } yield s2.language
}
