package xplat.lib.expr.core

import java.util.Date

trait DataExtractor extends DataExtractor.RowExtractor

object DataExtractor {

  trait RowExtractor {
    def getString(i: Int): Option[String]
    def getInt(i: Int): Option[Int]
    def getBoolean(i: Int): Option[Boolean]
    def getFloat(i: Int): Option[Float]
    def getDate(i: Int): Option[Date]
  }

}
