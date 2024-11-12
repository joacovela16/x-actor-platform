package xplat.lib.optimizer

trait ValidType {}

object ValidType {
  case object Number extends ValidType
  case object String extends ValidType
}
