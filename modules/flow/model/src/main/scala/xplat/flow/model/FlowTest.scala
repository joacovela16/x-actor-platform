package xplat.flow.model

import io.circe.{Decoder, Encoder, Json}

/** A test case of a flow, kept with the flow: run it in a simulation with these inputs and mocks, and check what came out. That way a
  * change to a flow that breaks what it did is seen when the flow is saved, or in CI.
  *
  * @param mocks
  *   the answer of each node that is mocked, as the simulation endpoint takes it (`{"values": ..., "port": ...}`, `{"error": ...}`, or
  *   `{"elements": [...]}` for a stream source).
  * @param expect
  *   what has to hold after the run.
  */
final case class FlowTest(name: String, inputs: Json = Json.obj(), mocks: Map[String, Json] = Map.empty, expect: FlowTest.Expect = FlowTest.Expect())

object FlowTest {

  /** @param status
    *   how the instance ends: `succeeded` or `failed`.
    * @param state
    *   what the global state must contain at the end. Only what is written here is checked: an object matches one that has at least
    *   its fields (with values that match); anything else has to be equal.
    * @param visited
    *   nodes that must have run (or been mocked), in no particular order.
    * @param notVisited
    *   nodes that must not have been reached: the branch that should not be taken.
    */
  final case class Expect(status: Option[String] = None, state: Option[Json] = None, visited: List[String] = Nil, notVisited: List[String] = Nil)

  /** What went wrong, or nothing if the test passed. `found` is the state at the end of the run. */
  final case class Result(name: String, passed: Boolean, problems: List[String], status: String, found: Json)

  /** Whether `found` contains `expected` (see [[Expect.state]]), and where it does not, as `path: why`. */
  def contains(expected: Json, found: Json, at: String = "$"): List[String] =
    (expected.asObject, found.asObject) match {
      case (Some(e), Some(f)) =>
        e.toList.flatMap { case (k, v) =>
          val path = if (at == "$") k else s"$at.$k"
          f(k) match {
            case None    => List(s"$path: missing (expected ${v.noSpaces})")
            case Some(x) => contains(v, x, path)
          }
        }
      case (Some(_), None) => List(s"$at: expected an object, found ${found.noSpaces}")
      case _               => if (expected == found) Nil else List(s"$at: expected ${expected.noSpaces}, found ${found.noSpaces}")
    }

  /** The problems of a run: `status` is how it ended, `ran` the nodes that ran or were mocked, `state` the state at the end. */
  def check(t: FlowTest, status: String, ran: Set[String], state: Json): Result = {
    val problems =
      t.expect.status.filter(_ != status).map(s => s"status: expected $s, found $status").toList ++
        t.expect.state.toList.flatMap(contains(_, state)) ++
        t.expect.visited.filterNot(ran).map(n => s"node '$n' did not run") ++
        t.expect.notVisited.filter(ran).map(n => s"node '$n' ran, and it should not have")
    Result(t.name, problems.isEmpty, problems, status, state)
  }

  implicit val expectEncoder: Encoder[Expect] = Encoder.instance { e =>
    Json.fromFields(
      e.status.map("status" -> Json.fromString(_)).toList ++ e.state.map("state" -> _).toList ++
        (if (e.visited.isEmpty) Nil else List("visited" -> Encoder[List[String]].apply(e.visited))) ++
        (if (e.notVisited.isEmpty) Nil else List("notVisited" -> Encoder[List[String]].apply(e.notVisited)))
    )
  }

  implicit val expectDecoder: Decoder[Expect] = Decoder.instance { c =>
    for {
      status <- c.get[Option[String]]("status")
      _ <- status.filterNot(Set("succeeded", "failed")).fold[Decoder.Result[Unit]](Right(()))(s => Left(io.circe.DecodingFailure(s"unknown status '$s'", c.history)))
      state <- c.get[Option[Json]]("state")
      visited <- c.getOrElse[List[String]]("visited")(Nil)
      notVisited <- c.getOrElse[List[String]]("notVisited")(Nil)
    } yield Expect(status, state, visited, notVisited)
  }

  implicit val encoder: Encoder[FlowTest] =
    Encoder.forProduct4("name", "inputs", "mocks", "expect")((t: FlowTest) => (t.name, t.inputs, t.mocks, t.expect))

  implicit val decoder: Decoder[FlowTest] = Decoder.instance { c =>
    for {
      name <- c.get[String]("name")
      inputs <- c.getOrElse[Json]("inputs")(Json.obj())
      mocks <- c.getOrElse[Map[String, Json]]("mocks")(Map.empty)
      expect <- c.getOrElse[Expect]("expect")(Expect())
    } yield FlowTest(name, inputs, mocks, expect)
  }

  implicit val resultEncoder: Encoder[Result] =
    Encoder.forProduct5("name", "passed", "problems", "status", "state")((r: Result) => (r.name, r.passed, r.problems, r.status, r.found))
}
