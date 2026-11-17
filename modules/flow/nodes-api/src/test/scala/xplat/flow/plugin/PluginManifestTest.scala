package xplat.flow.plugin

import io.circe.parser.decode
import org.scalatest.freespec.AnyFreeSpec
import org.scalatest.matchers.should.Matchers
import xplat.flow.plugin.PluginManifest.{Transport, Version}

class PluginManifestTest extends AnyFreeSpec with Matchers {

  "reads a jvm manifest; the transport is jvm when not given" in {
    val m = decode[PluginManifest]("""{"name":"ipa","version":"1.4.2","apiVersion":"1.0.0"}""").toOption.get
    m shouldBe PluginManifest("ipa", Version(1, 4, 2), Version(1, 0, 0), Transport.Jvm)
    m.kindName("lookup") shouldBe "ipa.lookup@1"
    m.id shouldBe "ipa@1"
  }

  "a process plugin needs its command" in {
    decode[PluginManifest]("""{"name":"geo","version":"0.3.0","apiVersion":"1.0.0","transport":"process"}""").isLeft shouldBe true
    decode[PluginManifest]("""{"name":"geo","version":"0.3.0","apiVersion":"1.0.0","transport":"process","command":["python3","main.py"]}""")
      .map(_.command) shouldBe Right(List("python3", "main.py"))
  }

  "rejects names that cannot prefix a kind, and versions that are not major.minor.patch" in {
    decode[PluginManifest]("""{"name":"Ipa.x","version":"1.0.0","apiVersion":"1.0.0"}""").isLeft shouldBe true
    decode[PluginManifest]("""{"name":"ipa","version":"1.0","apiVersion":"1.0.0"}""").isLeft shouldBe true
    Version.parse("2.0.0-rc.1") shouldBe Right(Version(2, 0, 0, "-rc.1"))
  }

  "the API version is a version" in {
    Version.parse(FlowPlugin.ApiVersion).isRight shouldBe true
  }
}
