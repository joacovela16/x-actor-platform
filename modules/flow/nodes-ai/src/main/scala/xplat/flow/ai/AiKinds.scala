package xplat.flow.ai

import xplat.flow.nodes.ConnectionKind
import io.circe.Json
import xplat.flow.model.{ConfigSchema, DataType}

/** The kinds of connection to an AI service. `claude` speaks the Messages API of Anthropic; `openai` and `codex` speak the Chat
  * Completions API that OpenAI and many other services offer (so `baseUrl` can point to one of those). The API key is a secret: it
  * never leaves the server, and it is not in the audit.
  */
object AiKinds {

  val Claude: String = "claude"
  val OpenAi: String = "openai"
  val Codex: String = "codex"

  /** The model used when neither the node nor the connection names one. The store of connections does not fill in the defaults of the
    * schema, so the node asks for them here.
    */
  val defaultModels: Map[String, String] = Map(Claude -> "claude-sonnet-5-5", OpenAi -> "gpt-4.1", Codex -> "codex-mini-latest")

  private def kind(id: String, label: String, baseUrl: String, model: String, extra: List[ConfigSchema.Field] = Nil): ConnectionKind =
    ConnectionKind(
      id,
      label,
      ConfigSchema(
        List(
          ConfigSchema.Field("baseUrl", DataType.Str, required = false, default = Some(Json.fromString(baseUrl)), description = Some("Base URL of the service")),
          ConfigSchema.Field("apiKey", DataType.Str, secret = true, description = Some("API key")),
          ConfigSchema.Field("model", DataType.Str, required = false, default = Some(Json.fromString(model)), description = Some("Model to use when the node does not name one"))
        ) ++ extra
      )
    )

  val claude: ConnectionKind = kind(
    Claude,
    "Claude (Anthropic)",
    "https://api.anthropic.com",
    defaultModels(Claude),
    List(ConfigSchema.Field("anthropicVersion", DataType.Str, required = false, default = Some(Json.fromString("2023-06-01")), description = Some("Value of the anthropic-version header")))
  )
  val openai: ConnectionKind = kind(OpenAi, "OpenAI-compatible", "https://api.openai.com/v1", defaultModels(OpenAi))
  val codex: ConnectionKind = kind(Codex, "Codex (OpenAI-compatible)", "https://api.openai.com/v1", defaultModels(Codex))

  val all: List[ConnectionKind] = List(claude, openai, codex)
}
