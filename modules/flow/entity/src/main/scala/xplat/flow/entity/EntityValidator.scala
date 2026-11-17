package xplat.flow.entity

import xplat.flow.model.{DataType, Expr}

/** Checks the definition of an entity before it is saved, without any value. It reports every problem it finds.
  *
  * What it checks: the version; names of the entity and of its fields; the key of a root; that the entities its types name exist
  * and that references point to roots; that no value would have to hold itself forever; that constraints and defaults fit the
  * type of their field; that expressions only read fields that exist and computed fields do not read each other in a circle; and
  * that the states and transitions are consistent.
  */
object EntityValidator {

  private val EntityName = "^[A-Za-z][A-Za-z0-9_]*$".r
  private val FieldName = "^[A-Za-z_][A-Za-z0-9_]*$".r

  /** The problems of `entity` among the entities of `catalog`, where it takes the place of the one with its name. */
  def validate(entity: EntityDef, catalog: EntityCatalog): List[EntityError] = {
    val all = catalog.updated(entity)
    val n = entity.name
    def error(code: String, message: String) = EntityError(code, n, message)
    def onField(code: String, field: String, message: String) = EntityError.onField(code, n, field, message)

    val version =
      if (entity.version == EntityDef.CurrentVersion) Nil
      else List(error("unsupported-version", s"version ${entity.version} of the format is not supported; this server reads version ${EntityDef.CurrentVersion}"))
    val name = if (EntityName.matches(n)) Nil else List(error("invalid-name", s"'$n' is not a valid name: a letter, then letters, digits or _"))
    val empty = if (entity.fields.isEmpty) List(error("no-fields", "an entity needs at least one field")) else Nil
    val names = entity.fields.map(_.name)
    val fieldNames = names.filterNot(FieldName.matches).map(f => onField("invalid-field-name", f, s"'$f' is not a valid field name"))
    val duplicates = names.diff(names.distinct).distinct.map(f => onField("duplicate-field", f, s"there are several fields called '$f'"))

    val key = (entity.kind, entity.key) match {
      case (EntityDef.Kind.Root, None) => List(error("missing-key", "a root entity needs a key: the field that identifies its values"))
      case (EntityDef.Kind.Value, Some(k)) => List(onField("invalid-key", k, "a value entity has no key: it only lives inside others"))
      case (EntityDef.Kind.Root, Some(k)) =>
        entity.field(k) match {
          case None => List(onField("unknown-key", k, s"the key '$k' is not a field"))
          case Some(f) if f.tpe != DataType.Str && f.tpe != DataType.Num =>
            List(onField("invalid-key", k, s"the key must be str or num, not ${f.tpe.render}"))
          case Some(f) if !f.required || f.computed.isDefined => List(onField("invalid-key", k, "the key must be required and not computed"))
          case Some(_) => Nil
        }
      case (EntityDef.Kind.Value, None) => Nil
    }

    val types = entity.fields.flatMap { f =>
      val unknown = f.tpe.entities.toList.sorted.filter(all.get(_).isEmpty).map(e => onField("unknown-entity", f.name, s"the entity '$e' does not exist"))
      val toValues = refs(f.tpe).distinct.filter(all.get(_).exists(_.kind == EntityDef.Kind.Value))
        .map(e => onField("ref-to-value", f.name, s"'$e' is a value entity: it can be held, not referenced"))
      unknown ++ toValues
    }

    val cycle = embeddingCycle(entity, all).toList.map(chain =>
      error("embedding-cycle", s"a value would hold itself forever: ${chain.mkString(" -> ")}; make one of those fields optional or a list")
    )

    val fields = entity.fields.flatMap { f =>
      val constraints = checkConstraints(f).map(onField("invalid-constraint", f.name, _))
      val default = f.default.toList.flatMap { d =>
        if (f.computed.isDefined) List(onField("invalid-default", f.name, "a computed field has no default"))
        else if (!f.tpe.accepts(d, all)) List(onField("invalid-default", f.name, s"the default ${d.noSpaces} is not ${f.tpe.render}"))
        else Nil
      }
      val oneOf = f.constraints.oneOf.filterNot(f.tpe.accepts(_, all))
        .map(v => onField("invalid-constraint", f.name, s"the value ${v.noSpaces} of oneOf is not ${f.tpe.render}"))
      val computed = f.computed.toList.flatMap(expression(_, entity, all)).map(m => onField("invalid-expression", f.name, s"computed: $m"))
      constraints ++ default ++ oneOf ++ computed
    }
    val circle = EntityCatalog.computeCycle(entity) match {
      case Nil   => Nil
      case names => List(error("computed-cycle", s"computed fields read each other in a circle: ${names.mkString(", ")}"))
    }

    val ruleNames = entity.rules.map(_.name)
    val rules = ruleNames.filter(_.trim.isEmpty).distinct.map(_ => error("invalid-rule", "a rule needs a name")) ++
      ruleNames.diff(ruleNames.distinct).distinct.map(r => error("duplicate-rule", s"there are several rules called '$r'")) ++
      entity.rules.flatMap { r =>
        (r.when.toList.flatMap(expression(_, entity, all)).map(m => s"when: $m") ++ expression(r.check, entity, all))
          .map(m => error("invalid-expression", s"rule '${r.name}': $m"))
      }

    val states = entity.states.toList.flatMap(checkStates(_, entity, all).map(error("invalid-states", _)))

    version ++ name ++ empty ++ fieldNames ++ duplicates ++ key ++ types ++ cycle ++ fields ++ circle ++ rules ++ states
  }

  /** The entities that `tpe` references, at any depth. */
  private def refs(tpe: DataType): List[String] = tpe match {
    case DataType.Ref(e)     => List(e)
    case DataType.ListOf(t)  => refs(t)
    case DataType.Obj(fs)    => fs.values.toList.flatMap(refs)
    case _                   => Nil
  }

  /** The entities that a value of `tpe` must always hold: inside a list or behind a reference there may be none. */
  private def held(tpe: DataType): List[String] = tpe match {
    case DataType.Entity(e) => List(e)
    case DataType.Obj(fs)   => fs.values.toList.flatMap(held)
    case _                  => Nil
  }

  /** A chain of entities from `entity` back to itself through fields that are always there, if there is one. */
  private def embeddingCycle(entity: EntityDef, all: EntityCatalog): Option[List[String]] = {
    def next(name: String): List[String] =
      all.get(name).toList.flatMap(_.fields.filter(f => f.required && f.computed.isEmpty && f.default.isEmpty)).flatMap(f => held(f.tpe)).distinct

    def search(current: String, chain: List[String], seen: Set[String]): Option[List[String]] =
      next(current).iterator.map { n =>
        if (n == entity.name) Some(chain :+ n)
        else if (seen(n)) None
        else search(n, chain :+ n, seen + n)
      }.collectFirst { case Some(c) => c }

    search(entity.name, List(entity.name), Set(entity.name))
  }

  private def expression(expr: Expr, entity: EntityDef, all: EntityCatalog): List[String] =
    expr.check(DataType.Entity(entity.name), all)

  private def checkConstraints(f: EntityDef.Field): List[String] = {
    val k = f.constraints
    def onlyFor(set: Boolean, name: String, fits: Boolean, what: String): List[String] =
      if (set && !fits) List(s"'$name' only applies to $what, and the field is ${f.tpe.render}") else Nil
    val isNum = f.tpe == DataType.Num
    val isText = f.tpe == DataType.Str
    val isList = f.tpe.isInstanceOf[DataType.ListOf]
    val applies =
      onlyFor(k.min.isDefined, "min", isNum, "numbers") ++ onlyFor(k.max.isDefined, "max", isNum, "numbers") ++
        onlyFor(k.minLength.isDefined, "minLength", isText, "text") ++ onlyFor(k.maxLength.isDefined, "maxLength", isText, "text") ++
        onlyFor(k.pattern.isDefined, "pattern", isText, "text") ++
        onlyFor(k.minItems.isDefined, "minItems", isList, "lists") ++ onlyFor(k.maxItems.isDefined, "maxItems", isList, "lists")
    val negative = List("minLength" -> k.minLength, "maxLength" -> k.maxLength, "minItems" -> k.minItems, "maxItems" -> k.maxItems)
      .collect { case (name, Some(v)) if v < 0 => s"'$name' cannot be negative" }
    val ranges = List(
      (k.min, k.max).mapN2((a, b) => a > b).filter(identity).map(_ => "'min' is greater than 'max'"),
      (k.minLength.map(BigDecimal(_)), k.maxLength.map(BigDecimal(_))).mapN2((a, b) => a > b).filter(identity).map(_ => "'minLength' is greater than 'maxLength'"),
      (k.minItems.map(BigDecimal(_)), k.maxItems.map(BigDecimal(_))).mapN2((a, b) => a > b).filter(identity).map(_ => "'minItems' is greater than 'maxItems'")
    ).flatten
    val pattern = k.pattern.toList.flatMap(p => Expr.matches("", p).left.toOption)
    applies ++ negative ++ ranges ++ pattern
  }

  private def checkStates(m: EntityDef.StateMachine, entity: EntityDef, all: EntityCatalog): List[String] = {
    val field = entity.field(m.field) match {
      case None                                  => List(s"the state field '${m.field}' is not a field")
      case Some(f) if f.tpe != DataType.Str      => List(s"the state field '${m.field}' must be str, not ${f.tpe.render}")
      case Some(f) if f.computed.isDefined       => List(s"the state field '${m.field}' cannot be computed")
      case Some(_)                               => Nil
    }
    val known = m.states.toSet
    val states =
      (if (m.states.isEmpty) List("there are no states") else Nil) ++
        m.states.diff(m.states.distinct).distinct.map(s => s"the state '$s' is listed more than once") ++
        (if (m.states.nonEmpty && !known(m.initial)) List(s"the initial state '${m.initial}' is not one of the states") else Nil)
    val names = m.transitions.map(_.name)
    val transitions =
      names.filter(_.trim.isEmpty).distinct.map(_ => "a transition needs a name") ++
        names.diff(names.distinct).distinct.map(t => s"there are several transitions called '$t'") ++
        m.transitions.flatMap { t =>
          val from = if (t.from.isEmpty) List(s"the transition '${t.name}' starts from no state") else Nil
          val unknown = (t.from :+ t.to).distinct.filterNot(known).map(s => s"the transition '${t.name}' uses the state '$s', which is not one of the states")
          val guard = t.guard.toList.flatMap(expression(_, entity, all)).map(msg => s"the guard of '${t.name}': $msg")
          from ++ unknown ++ guard
        }
    field ++ states ++ transitions
  }

  private implicit class PairOps[A](private val pair: (Option[A], Option[A])) extends AnyVal {
    def mapN2[B](f: (A, A) => B): Option[B] = for { a <- pair._1; b <- pair._2 } yield f(a, b)
  }
}
