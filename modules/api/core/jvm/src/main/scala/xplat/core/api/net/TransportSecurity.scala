package xplat.core.api.net

/** Protection of the connections between processes (cluster nodes, lease server and its clients).
  *
  * @param tls
  *   encrypts the connections and checks the certificate of the other side (mutual TLS).
  * @param sharedSecret
  *   every connection must prove it knows this secret before any message is accepted. The secret itself never travels:
  *   each side answers a random challenge with an HMAC of it. Use it together with TLS, which also hides the messages.
  */
final case class TransportSecurity(tls: Option[TlsConfig] = None, sharedSecret: Option[String] = None)

/** Key and trust stores in PKCS12 or JKS format. Every process presents its certificate and only accepts certificates
  * signed by an authority in the trust store.
  */
final case class TlsConfig(
    keyStore: java.nio.file.Path,
    keyStorePassword: String,
    trustStore: java.nio.file.Path,
    trustStorePassword: String,
    keyPassword: Option[String] = None
)
