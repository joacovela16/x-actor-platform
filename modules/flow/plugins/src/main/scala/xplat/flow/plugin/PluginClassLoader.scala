package xplat.flow.plugin

import java.net.{URL, URLClassLoader}

/** The class loader of one plugin: child-first, so a plugin can bring its own version of any library, except for the
  * packages of the API and what it exposes (Scala, cats, cats-effect, circe), which always come from the server so that
  * the types of both sides are the same classes.
  */
final class PluginClassLoader(name: String, urls: Array[URL], parent: ClassLoader) extends URLClassLoader(s"flow-plugin-$name", urls, parent) {

  private def parentFirst(className: String): Boolean = PluginClassLoader.Shared.exists(className.startsWith)

  override protected def loadClass(className: String, resolve: Boolean): Class[_] =
    getClassLoadingLock(className).synchronized {
      val c = Option(findLoadedClass(className)).getOrElse {
        if (parentFirst(className)) super.loadClass(className, false)
        else
          try findClass(className)
          catch { case _: ClassNotFoundException => super.loadClass(className, false) }
      }
      if (resolve) resolveClass(c)
      c
    }

  override def getResource(resource: String): URL = Option(findResource(resource)).getOrElse(super.getResource(resource))
}

object PluginClassLoader {

  /** Packages that always come from the server. */
  val Shared: List[String] = List(
    "java.",
    "javax.",
    "jdk.",
    "sun.",
    "scala.",
    "cats.",
    "io.circe.",
    "org.typelevel.",
    "org.slf4j.",
    "xplat.flow.model.",
    "xplat.flow.nodes.",
    "xplat.flow.plugin."
  )
}
