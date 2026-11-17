package xplat.flow.ai

import cats.effect.Async
import cats.effect.syntax.temporal._
import cats.syntax.all._
import io.circe.parser.parse
import io.circe.{Decoder, Json}
import xplat.flow.model._
import xplat.flow.nodes.{Connections, NodeType}

import java.util.concurrent.TimeoutException
import scala.concurrent.duration._

/** Asks an AI model, over a named connection (`claude`, `openai` or `codex`; see [[AiKinds]]), and writes its answer in the state.
  *
  * The prompt (and the optional system prompt) are text with `${state.path}` where values of the state go. The answer is the output `text`;
  * with `json: true` the model is asked for a JSON value and it is the output `json`, checked against `schema` (a data type) when there is
  * one, so a flow can branch on what the model said. A node can also declare `usage` (`{inputTokens, outputTokens}`). It answers only
  * what it declared.
  *
  * **Errors and retries.** A 429, a 5xx, a timeout or a network error is retried up to `retries` times, waiting longer each time (or what
  * `Retry-After` says); any other error, an answer that is not JSON when it must be, or one that does not fit the schema, fails the node
  * through `error`. **Cost.** `maxTokens` caps the answer; `maxTotalTokens` caps input plus output: a prompt that is clearly larger is not
  * sent, the answer is shortened to what is left, and a run that used more fails. The use of tokens is in the `info` of the audit.
  * **Secrets.** The API key is only in the header of the request: it is not in the audit nor in any message.
  *
  * Tools (tool use) are not supported yet. It reads from an outside system, so a simulation does not call it: it answers
  * `simulatedResponse` if the node has one, or a value of the right type, and shows the prompt that it would have sent.
  */
final class AiNode[F[_]](connections: Connections[F], transport: AiTransport[F], settings: AiNode.Settings = AiNode.Settings())(implicit F: Async[F]) extends NodeType[F] {

  val descriptor: NodeDescriptor = AiNode.descriptor

  def run(input: NodeInput): F[NodeOutput] =
    AiNode.prepare(input) match {
      case Left(problem) => NodeOutput.failed(problem).pure[F]
      case Right(prepared) =>
        connections.resolve(prepared.params.connection).flatMap {
          case None => NodeOutput.failed(s"the connection '${prepared.params.connection}' does not exist").pure[F]
          case Some(c) =>
            (for {
              provider <- AiProvider.of(c.kind).toRight(s"the connection '${c.name}' is of kind '${c.kind}', which is not an AI service")
              apiKey <- c.secrets.get("apiKey").filter(_.nonEmpty).toRight(s"the connection '${c.name}' has no apiKey")
              model <- prepared.params.model.orElse(c.text("model")).orElse(AiKinds.defaultModels.get(c.kind)).toRight("no model: name one in the node or in the connection")
            } yield (provider, apiKey, model)) match {
              case Left(problem) => NodeOutput.failed(problem).pure[F]
              case Right((provider, apiKey, model)) =>
                val request = provider.request(c, apiKey, prepared.call(model))
                val started = System.nanoTime()
                attempts(provider, request, prepared.params).map {
                  case Left(problem) => NodeOutput.failed(AiNode.clean(problem, apiKey))
                  case Right((answer, tries)) =>
                    AiNode.finish(prepared, input, model, c.name, answer, tries, (System.nanoTime() - started) / 1000000)
                }.handleError(e => NodeOutput.failed(AiNode.clean(s"the call failed: ${Option(e.getMessage).getOrElse(e.toString)}", apiKey)))
            }
        }
    }

  private def attempts(provider: AiProvider, request: AiProvider.Request, p: AiNode.Params): F[Either[String, (AiProvider.Answer, Int)]] = {
    def pause(n: Int, retryAfter: Option[Long]): F[Unit] =
      F.sleep(retryAfter.map(s => (s.seconds min 30.seconds)).getOrElse((settings.backoff * math.pow(2, n.toDouble).toLong) min 8.seconds))

    def go(n: Int): F[Either[String, (AiProvider.Answer, Int)]] =
      transport.post(request.url, request.headers, request.body).timeout(p.timeoutSeconds.seconds).attempt.flatMap {
        case Right(reply) if reply.status / 100 == 2 => F.pure(provider.answer(reply.body).map(_ -> (n + 1)))
        case Right(reply) if AiNode.retryable(reply.status) && n < p.retries => pause(n, reply.retryAfterSeconds) >> go(n + 1)
        case Right(reply) => F.pure(Left(s"the service answered ${reply.status}: ${provider.errorMessage(reply.body).getOrElse("no message")}"))
        case Left(_) if n < p.retries => pause(n, None) >> go(n + 1)
        case Left(_: TimeoutException) => F.pure(Left(s"the service did not answer in ${p.timeoutSeconds} seconds"))
        case Left(e) => F.pure(Left(s"cannot reach the service: ${Option(e.getMessage).getOrElse(e.toString)}"))
      }

    go(0)
  }

  override def simulated(input: NodeInput): Option[NodeOutput] =
    Some(AiNode.prepare(input) match {
      case Left(problem) => NodeOutput.failed(problem)
      case Right(prepared) =>
        val text = prepared.params.simulatedResponse.getOrElse(if (prepared.params.json) AiNode.defaultFor(prepared.params.schema).noSpaces else "[simulated answer]")
        val answer = AiProvider.Answer(text, 0L, 0L, Some("simulated"))
        val out = AiNode.finish(prepared, input, prepared.params.model.getOrElse("(model of the connection)"), prepared.params.connection, answer, 0, 0L)
        out.copy(info = out.info ++ Map("simulated" -> Json.True, "prompt" -> Json.fromString(prepared.prompt)) ++ prepared.system.map(s => "system" -> Json.fromString(s)))
    })
}

object AiNode {

  val Kind: String = "ai.agent"

  /** @param backoff
    *   the wait before the first retry; it doubles each time, up to 8 seconds. Tests make it short.
    */
  final case class Settings(backoff: FiniteDuration = 500.millis)

  final case class Params(
      connection: String,
      prompt: String,
      system: Option[String],
      model: Option[String],
      maxTokens: Int,
      temperature: Option[Double],
      timeoutSeconds: Int,
      retries: Int,
      json: Boolean,
      schema: Option[DataType],
      maxTotalTokens: Option[Long],
      simulatedResponse: Option[String]
  )

  def params(json: Json): Either[String, Params] = {
    val c = json.hcursor
    def bad(e: io.circe.DecodingFailure) = e.message
    for {
      connection <- c.get[String]("connection").left.map(_ => "missing field 'connection'").filterOrElse(_.nonEmpty, "connection is empty")
      prompt <- c.get[String]("prompt").left.map(_ => "missing field 'prompt'")
      system <- c.get[Option[String]]("system").left.map(bad)
      model <- c.get[Option[String]]("model").left.map(bad)
      maxTokens <- c.getOrElse[Int]("maxTokens")(1024).left.map(bad).filterOrElse(n => n >= 1 && n <= 200000, "maxTokens must be between 1 and 200000")
      temperature <- c.get[Option[Double]]("temperature").left.map(bad).filterOrElse(_.forall(t => t >= 0 && t <= 2), "temperature must be between 0 and 2")
      timeout <- c.getOrElse[Int]("timeoutSeconds")(60).left.map(bad).filterOrElse(n => n >= 1 && n <= 3600, "timeoutSeconds must be between 1 and 3600")
      retries <- c.getOrElse[Int]("retries")(2).left.map(bad).filterOrElse(n => n >= 0 && n <= 10, "retries must be between 0 and 10")
      asJson <- c.getOrElse[Boolean]("json")(false).left.map(bad)
      schema <- c.get[Option[DataType]]("schema").left.map(e => s"schema: ${bad(e)}")
      _ <- Either.cond(asJson || schema.isEmpty, (), "schema only applies with json: true")
      budget <- c.get[Option[Long]]("maxTotalTokens").left.map(bad).filterOrElse(_.forall(_ > 0), "maxTotalTokens must be positive")
      simulated <- c.get[Option[String]]("simulatedResponse").left.map(bad)
    } yield Params(connection, prompt, system.filter(_.nonEmpty), model.filter(_.nonEmpty), maxTokens, temperature, timeout, retries, asJson, schema, budget, simulated)
  }

  /** The configuration with the values of the state in the prompts. */
  final case class Prepared(params: Params, prompt: String, system: Option[String]) {

    /** The estimate (4 characters a token) is only used to refuse what is clearly too large. */
    def estimatedInputTokens: Long = (prompt.length + system.fold(0)(_.length)) / 4L + 1

    /** The answer is never asked to be longer than what is left of the budget. */
    def maxTokens: Int = params.maxTotalTokens.fold(params.maxTokens)(b => math.max(1L, math.min(params.maxTokens.toLong, b - estimatedInputTokens)).toInt)

    def call(model: String): AiProvider.Call = AiProvider.Call(model, system, prompt, maxTokens, params.temperature, params.json)
  }

  def prepare(input: NodeInput): Either[String, Prepared] =
    for {
      p <- params(input.params)
      prompt <- Template.render(p.prompt, input.state)
      system <- p.system.traverse(Template.render(_, input.state))
      prepared = Prepared(p, prompt, system)
      _ <- p.maxTotalTokens.toLeft(()).left.flatMap(b => Either.cond(prepared.estimatedInputTokens <= b, (), s"the prompt is about ${prepared.estimatedInputTokens} tokens, more than maxTotalTokens ($b)"))
    } yield prepared

  def retryable(status: Int): Boolean = status == 429 || status >= 500

  private[ai] def clean(text: String, secret: String): String = if (secret.isEmpty) text else text.replace(secret, "***")

  /** The JSON in the text of an answer: the text itself, what is between code fences, or what is between the first bracket and the last. */
  def extractJson(text: String): Either[String, Json] = {
    val trimmed = text.trim
    val unfenced = "(?s)^```[a-zA-Z]*\\s*(.*?)\\s*```$".r.findFirstMatchIn(trimmed).map(_.group(1)).getOrElse(trimmed)
    parse(unfenced).left.flatMap { _ =>
      val start = unfenced.indexWhere(c => c == '{' || c == '[')
      val end = unfenced.lastIndexWhere(c => c == '}' || c == ']')
      if (start >= 0 && end > start) parse(unfenced.substring(start, end + 1)).left.map(_ => "the answer is not valid JSON") else Left("the answer is not valid JSON")
    }
  }

  /** A value of the type, for a simulation that has no answer to give. */
  def defaultFor(schema: Option[DataType]): Json = schema.fold(Json.obj())(default)

  private def default(t: DataType): Json = t match {
    case DataType.Bool      => Json.False
    case DataType.Num       => Json.fromInt(0)
    case DataType.Str       => Json.fromString("")
    case DataType.AnyJson   => Json.obj()
    case DataType.ListOf(_) => Json.arr()
    case DataType.Obj(fs)   => Json.fromFields(fs.map { case (k, v) => k -> default(v) })
  }

  /** The node's output for an answer: the values it declared, and the use of tokens in the audit. */
  private[ai] def finish(p: Prepared, input: NodeInput, model: String, connection: String, answer: AiProvider.Answer, tries: Int, millis: Long): NodeOutput = {
    val used = answer.inputTokens + answer.outputTokens
    val usage = Json.obj("inputTokens" -> Json.fromLong(answer.inputTokens), "outputTokens" -> Json.fromLong(answer.outputTokens))
    val info = Map(
      "connection" -> Json.fromString(connection),
      "model" -> Json.fromString(model),
      "usage" -> usage,
      "attempts" -> Json.fromInt(tries),
      "durationMs" -> Json.fromLong(millis)
    ) ++ answer.stop.map(s => "stop" -> Json.fromString(s))
    def failed(reason: String) = NodeOutput.failed(reason).copy(info = info)

    val parsed: Either[String, Option[Json]] =
      if (!p.params.json) Right(None)
      else extractJson(answer.text).flatMap(j => Either.cond(p.params.schema.forall(_.accepts(j)), Some(j), s"the answer does not fit ${p.params.schema.map(_.render).getOrElse("the schema")}"))
    val all: Map[String, Json] = Map("text" -> Json.fromString(answer.text), "usage" -> usage) ++ parsed.toOption.flatten.map("json" -> _)
    val wanted = input.outputs.map(_.name)
    val available = if (p.params.json) all else all - "json"
    val unknown = wanted.filterNot(available.contains)

    p.params.maxTotalTokens match {
      case Some(b) if used > b => failed(s"the run used $used tokens, more than maxTotalTokens ($b)")
      case _ =>
        parsed match {
          case Left(reason)                           => failed(reason)
          case Right(_) if unknown.nonEmpty           => failed(s"ai.agent cannot answer ${unknown.map(n => s"'$n'").mkString(", ")}; it answers ${available.keys.toList.sorted.mkString(", ")}")
          case Right(_) =>
            val values = if (wanted.isEmpty) (available - "usage") else available.filter { case (k, _) => wanted.contains(k) }
            NodeOutput(values, info = info)
        }
    }
  }

  private def templatePaths(p: Params): Either[String, List[StatePath]] =
    for {
      a <- Template.paths(p.prompt)
      b <- p.system.fold[Either[String, List[StatePath]]](Right(Nil))(Template.paths)
    } yield (a ++ b).distinct

  val descriptor: NodeDescriptor = NodeDescriptor(
    kind = Kind,
    category = "AI",
    label = "AI agent",
    description = "Asks an AI model with a prompt built from the state, and writes its answer (text, or JSON checked against a schema).",
    config = ConfigSchema(
      List(
        ConfigSchema.Field("connection", DataType.Str, description = Some("Name of the connection (claude, openai or codex)")),
        ConfigSchema.Field("prompt", DataType.Str, description = Some("What to ask. ${state.path} is replaced by a value of the state."), ui = Some(UiSpec.textarea(8))),
        ConfigSchema.Field("system", DataType.Str, required = false, description = Some("System prompt, also with ${state.path}")),
        ConfigSchema.Field("model", DataType.Str, required = false, description = Some("Model; the one of the connection if empty")),
        ConfigSchema.Field("maxTokens", DataType.Num, required = false, default = Some(Json.fromInt(1024)), description = Some("Most tokens in the answer")),
        ConfigSchema.Field("temperature", DataType.Num, required = false, description = Some("0 to 2")),
        ConfigSchema.Field("timeoutSeconds", DataType.Num, required = false, default = Some(Json.fromInt(60))),
        ConfigSchema.Field("retries", DataType.Num, required = false, default = Some(Json.fromInt(2)), description = Some("Retries after a 429, a 5xx or a timeout")),
        ConfigSchema.Field("json", DataType.Bool, required = false, default = Some(Json.False), description = Some("Ask for a JSON answer, written to the output json")),
        ConfigSchema.Field("schema", DataType.AnyJson, required = false, description = Some("Data type the JSON answer must fit, such as {\"type\": \"obj\", \"fields\": {...}}")),
        ConfigSchema.Field("maxTotalTokens", DataType.Num, required = false, description = Some("Most tokens, input plus output; a run that uses more fails")),
        ConfigSchema.Field("simulatedResponse", DataType.Str, required = false, description = Some("What the node answers in test mode"))
      )
    ),
    sideEffects = SideEffects.Read,
    defaultOutputs = _ => List(OutputDecl("text", StatePath(List("answer")), DataType.Str)),
    reads = json => params(json).toOption.flatMap(templatePaths(_).toOption).toList.flatten.map(StateRead(_, DataType.AnyJson)),
    checkParams = json =>
      params(json) match {
        case Left(problem) => List(problem)
        case Right(p)      => templatePaths(p).left.toOption.toList
      }
  )
}
