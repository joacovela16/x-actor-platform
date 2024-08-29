package xplat.lib.expr.core

import scala.reflect.macros.blackbox
import scala.language.experimental.macros

trait Metadata[M[_[_]]] {
  def tpe: Type.Clazz
  def query: Query.Aux[M[TypedAst], M[Commons.Id]]
}

object Metadata {
  implicit def mk[M[_[_]]]: Metadata[M] = macro genMetadataImpl[M]
  type JType = Type

  def genMetadataImpl[M[_[_]]](c: blackbox.Context)(implicit tw: c.WeakTypeTag[M[Any]]): c.Expr[Metadata[M]] = {
    import c.universe._
    val ExprType = typeOf[Expr[Any]]
    val ExprAccessType = symbolOf[TypedAst.Access[_]]
    val IdType = typeOf[Commons.Id[_]]
    val QueryType = symbolOf[Query[_]]
    val TypeSymbol = symbolOf[JType]
    val TypeSymbolCompanion = symbolOf[JType]
    val FieldSymbol = symbolOf[Field]
    val TypeClazzSymbol = symbolOf[Type.Clazz]
    val TypeMapperType = typeOf[TypeMapper[_]]

    val MType = weakTypeOf[M[Any]]
    MType.members.find(x => x.isConstructor).map(x => x.asMethod) match {
      case Some(constructor) =>
        val params = constructor.paramLists.flatten

        val baseFields = params.map { s =>
          val signature = s.typeSignature
          val argTpe = signature.typeArgs match {
            case Nil       => signature
            case head +: _ => head
          }
          s -> argTpe

        }

        val astFields = baseFields.map { case (s, argTpe) =>
          val fieldName: Literal = Literal(Constant(s.name.toString))
          q"$FieldSymbol($fieldName, ${TypeSymbolCompanion}.apply[$argTpe])"
        }
        val entityName: Literal = Literal(Constant(MType.typeSymbol.name.toString))
        val astTree = q"$TypeClazzSymbol($entityName, List(..$astFields))"

        val instanceFields = baseFields.zipWithIndex.map { case ((s, argTpe),i) =>
          val fieldName: Literal = Literal(Constant(s.name.toString))
          val fieldIndex = Literal(Constant(i))
          q"$s = $ExprAccessType[$argTpe]($fieldName, $entityName, $fieldIndex, $TypeMapperType.apply[$argTpe])"
        }
        val instanceTree = q"$MType[$ExprType](..$instanceFields)"
        println(instanceTree)
        val queryTree = q"$QueryType.apply[$MType, $MType[$ExprType], $MType[$IdType]]($astTree, $instanceTree)"
        c.Expr[Metadata[M]](queryTree)
      case None =>
        ???
    }
  }

}
