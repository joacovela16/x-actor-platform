package xplat.core.api.error

final case class MailboxOverflow(path: String, capacity: Int) extends RuntimeException(s"Mailbox of $path is full ($capacity messages)")
