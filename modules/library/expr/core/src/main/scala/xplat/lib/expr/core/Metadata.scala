package xplat.lib.expr.core

import scala.language.experimental.macros
import scala.reflect.macros.blackbox
import xplat.lib.expr.core.{Expr => JExpr, Type => JType}

final case class Metadata[M[_[_]]](tpe: Type.Clazz, query: Query.Aux[M[JExpr], M[Commons.Id]])

object Metadata {
  implicit def mk[M[_[_]]]: Metadata[M] = macro genMetadataImpl[M]


  def genMetadataImpl[M[_[_]]](c: blackbox.Context)(implicit tw: c.WeakTypeTag[M[Any]]): c.Expr[Metadata[M]] = {
    import c.universe._

    val TypedAstType = typeOf[JExpr[_]]
    val ExprAccessType = typeOf[JExpr.Access[_]]
    val IdType = symbolOf[Commons.Id[_]]
    val QueryType = typeOf[Query[_]]
    val TypeType = typeOf[JType]
    val FieldSymbol = typeOf[Field]
    val TypeClazzSymbol = typeOf[JType.Clazz]
    val TypeMapperType = typeOf[TypeMapper[_]]
    val MetadataType = typeOf[Metadata[Any]]
    val AstEntityType = typeOf[Ast.Entity]
    val AstType = typeOf[Ast]
    val MType = weakTypeOf[M[Any]]

    val MTypeSymbol = weakTypeOf[M[Any]].erasure.typeSymbol

    MType.members.find(x => x.isConstructor).map(x => x.asMethod) match {
      case Some(constructor) =>
        val params = constructor.paramLists.flatten

        val baseFields = params.map { s =>
          val signature = s.typeSignature
          val argTpe = signature.typeArgs match {
            case Nil => signature
            case head +: _ => head
          }
          s -> argTpe

        }

        val astFields = baseFields.map { case (s, argTpe) =>
          val fieldName: Literal = Literal(Constant(s.name.toString))
          q"${FieldSymbol.typeSymbol.companion}($fieldName, ${TypeType.typeSymbol.companion}[$argTpe])"
        }
        val entityName: Literal = Literal(Constant(MType.typeSymbol.name.toString))
        val astTree = q"${TypeClazzSymbol.typeSymbol.companion}($entityName, List(..$astFields))"

        val instanceFields = baseFields.zipWithIndex.map { case ((s, argTpe), i) =>
          val fieldName: Literal = Literal(Constant(s.name.toString))
          val fieldIndex = Literal(Constant(i))
          q"${ExprAccessType.typeSymbol.companion}[$argTpe]($fieldName, $entityName, $fieldIndex, ${TypeMapperType.typeSymbol.companion}[$argTpe])"
        }

        val cc = weakTypeOf[M[Any]].typeSymbol
        val instanceTree = q"new $cc[${TypedAstType.erasure.typeSymbol}](..$instanceFields)"
        val queryTree = q"""${QueryType.typeSymbol.companion}[$MTypeSymbol, ${TypedAstType.erasure.typeSymbol}, $IdType](${AstEntityType.typeSymbol.companion}(ast), inst)"""
        val result =
          q"""{
            lazy val ast = $astTree
            lazy val inst = $instanceTree
            lazy val qt = $queryTree
            ${MetadataType.typeSymbol.companion}[$MTypeSymbol](ast, qt)
           }
           """
        c.Expr[Metadata[M]](result)
      case None =>
        ???
    }
  }

}
