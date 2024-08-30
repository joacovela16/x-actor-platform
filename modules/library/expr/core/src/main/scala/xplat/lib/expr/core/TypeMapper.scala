package xplat.lib.expr.core

trait TypeMapper[T] {
  def tpe: Type
  def get(dataExtractor: DataExtractor, index: Int): Either[String, T]
  def set(dataStore: DataStore, index: Int): Either[String, DataStore]
}

object TypeMapper {
  def apply[T](implicit typeMapper: TypeMapper[T]): TypeMapper[T] = typeMapper

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
