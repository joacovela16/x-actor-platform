package xplat.flow.ai

import io.circe.Json
import xplat.flow.nodes.Connections

/** The two ways of talking to an AI service: the request that asks for a completion and the reading of its answer. */
sealed trait AiProvider {
  def request(connection: Connections.Resolved, apiKey: String, call: AiProvider.Call): AiProvider.Request
  def answer(body: Json): Either[String, AiProvider.Answer]

  /** The message that the service gave for an error, if there is one. */
  def errorMessage(body: Json): Option[String] =
    body.hcursor.downField("error").focus.flatMap(e => e.hcursor.get[String]("message").toOption.orElse(e.asString)).orElse(body.asString.map(_.take(300)))
}

object AiProvider {

  /** @param json
    *   the answer must be a JSON value.
    */
  final case class Call(model: String, system: Option[String], prompt: String, maxTokens: Int, temperature: Option[Double], json: Boolean)

  final case class Request(url: String, headers: List[(String, String)], body: Json)

  final case class Answer(text: String, inputTokens: Long, outputTokens: Long, stop: Option[String])

  private val JsonOnly = "Reply with a single JSON value and nothing else: no explanation and no code fences."

  private def base(c: Connections.Resolved, default: String): String = c.text("baseUrl").getOrElse(default).stripSuffix("/")

  case object Claude extends AiProvider {
    def request(c: Connections.Resolved, apiKey: String, call: Call): Request = {
      val system = (call.system.toList ++ (if (call.json) List(JsonOnly) else Nil)).mkString("\n\n")
      val body = Json.obj(
        "model" -> Json.fromString(call.model),
        "max_tokens" -> Json.fromInt(call.maxTokens),
        "messages" -> Json.arr(Json.obj("role" -> Json.fromString("user"), "content" -> Json.fromString(call.prompt)))
      )
      val withSystem = if (system.nonEmpty) body.mapObject(_.add("system", Json.fromString(system))) else body
      val withTemperature = call.temperature.fold(withSystem)(t => withSystem.mapObject(_.add("temperature", Json.fromDoubleOrNull(t))))
      Request(
        base(c, "https://api.anthropic.com") + "/v1/messages",
        List("x-api-key" -> apiKey, "anthropic-version" -> c.text("anthropicVersion").getOrElse("2023-06-01"), "content-type" -> "application/json"),
        withTemperature
      )
    }

    def answer(body: Json): Either[String, Answer] = {
      val c = body.hcursor
      val text = c.downField("content").focus.flatMap(_.asArray).map(_.flatMap(b => b.hcursor.get[String]("text").toOption).mkString)
      text.toRight("the answer has no text").map { t =>
        Answer(t, c.downField("usage").get[Long]("input_tokens").getOrElse(0L), c.downField("usage").get[Long]("output_tokens").getOrElse(0L), c.get[String]("stop_reason").toOption)
      }
    }
  }

  case object OpenAi extends AiProvider {
    def request(c: Connections.Resolved, apiKey: String, call: Call): Request = {
      val system = (call.system.toList ++ (if (call.json) List(JsonOnly) else Nil)).mkString("\n\n")
      def message(role: String, content: String) = Json.obj("role" -> Json.fromString(role), "content" -> Json.fromString(content))
      val messages = (if (system.nonEmpty) List(message("system", system)) else Nil) :+ message("user", call.prompt)
      val body = Json.obj("model" -> Json.fromString(call.model), "max_tokens" -> Json.fromInt(call.maxTokens), "messages" -> Json.fromValues(messages))
      val withTemperature = call.temperature.fold(body)(t => body.mapObject(_.add("temperature", Json.fromDoubleOrNull(t))))
      val withFormat = if (call.json) withTemperature.mapObject(_.add("response_format", Json.obj("type" -> Json.fromString("json_object")))) else withTemperature
      Request(base(c, "https://api.openai.com/v1") + "/chat/completions", List("authorization" -> s"Bearer $apiKey", "content-type" -> "application/json"), withFormat)
    }

    def answer(body: Json): Either[String, Answer] = {
      val c = body.hcursor
      val first = c.downField("choices").downArray
      first.downField("message").get[String]("content").left.map(_ => "the answer has no text").map { t =>
        Answer(t, c.downField("usage").get[Long]("prompt_tokens").getOrElse(0L), c.downField("usage").get[Long]("completion_tokens").getOrElse(0L), first.get[String]("finish_reason").toOption)
      }
    }
  }

  def of(kind: String): Option[AiProvider] = kind match {
    case AiKinds.Claude                  => Some(Claude)
    case AiKinds.OpenAi | AiKinds.Codex => Some(OpenAi)
    case _                               => None
  }
}
