package xplat.flow.server

import cats.effect.{ExitCode, IO, IOApp}

/** The standalone flow server. This is the only place that names `IO`: everything else is written over `F` and is given `IO`
  * here. See [[ServerConfig.usage]] for the settings; `bundle ...` runs [[BundleCli]] instead of the server.
  */
object AppIO extends IOApp {

  def run(args: List[String]): IO[ExitCode] =
    if (args.headOption.contains("bundle")) BundleCli.run[IO](args.tail)
    else if (args.contains("--help") || args.contains("-h")) IO.print(ServerConfig.usage).as(ExitCode.Success)
    else
      ServerConfig.parse(args, sys.env) match {
        case Left(problem) => IO.println(s"$problem\n\n${ServerConfig.usage}").as(ExitCode.Error)
        case Right(config) =>
          FlowServer
            .resource[IO](config)
            .use { server =>
              IO.println(s"flow server on ${server.baseUri}  (data: ${config.dataDir.toAbsolutePath}, instances: ${config.database.toString.toLowerCase})") *> IO.never
            }
            .as(ExitCode.Success)
      }
}
