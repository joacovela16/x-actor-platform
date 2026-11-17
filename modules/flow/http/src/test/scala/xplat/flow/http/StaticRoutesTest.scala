package xplat.flow.http

import org.http4s.Status

class StaticRoutesTest extends HttpSpec {

  "the UI is served from the classpath with a fallback to index.html" in withApp { app =>
    for {
      root <- app.get("/")
      route <- app.get("/flow/approval")
      asset <- app.get("/assets/app.js")
      missing <- app.get("/assets/missing.js")
      api <- app.get("/api/nope")
      traversal <- app.get("/assets/%2e%2e/index.html")
    } yield {
      root.status shouldBe Status.Ok
      root.text should include("flow ui")
      route.text should include("flow ui")
      asset.text should include("console.log")
      missing.status shouldBe Status.NotFound
      api.status shouldBe Status.NotFound
      traversal.status shouldBe Status.NotFound
    }
  }
}
