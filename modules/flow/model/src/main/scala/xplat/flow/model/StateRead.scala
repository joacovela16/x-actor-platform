package xplat.flow.model

/** A value that a node needs to find in the global state when it runs, with the type it expects. */
final case class StateRead(path: StatePath, tpe: DataType)
