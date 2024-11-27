package xplat.lib.optimizer.model.dsl

final case class ExpandedModel(
    kind: ObjectiveKind,
    objective: Expr.Numerical,
    constraints: LazyList[Expr.Predicate.Relation]
)
