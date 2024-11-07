package xplat.lib.optimizer


sealed trait Kind {}

object Kind {
   case object Integer extends Kind
   case object Boolean extends Kind
   case object Double extends Kind
   case object String extends Kind
}