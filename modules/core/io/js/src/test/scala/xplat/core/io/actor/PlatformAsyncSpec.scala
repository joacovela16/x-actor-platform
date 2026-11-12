package xplat.core.io.actor

import org.scalatest.AsyncTestSuite

import scala.concurrent.ExecutionContext

/** ScalaTest's default execution context on Scala.js is serial and fails the tests that wait on timers or other
  * asynchronous work, so the specs run on the event loop instead.
  */
trait PlatformAsyncSpec extends AsyncTestSuite {
  override implicit def executionContext: ExecutionContext = scala.scalajs.concurrent.JSExecutionContext.queue
}
