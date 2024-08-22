package io.exsql.datastore.controller

import java.nio.file.Paths

import com.twitter.util.Await
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

object Bootstrap extends App {

  private val logger = LoggerFactory.getLogger("io.exsql.datastore.controller.Bootstrap")

  private val config = ConfigFactory
    .parseFile(Paths.get(args(0)).toAbsolutePath.toFile)
    .resolve()
    .withFallback(ConfigFactory.defaultReference(Bootstrap.getClass.getClassLoader))

  logger.info("starting exsql datastore controller node")
  logger.info(s"release information:")
  logger.info(s"  controller(${io.exsql.datastore.controller.BuildInfo})")

  val server = ControllerServer(config)
  val listeningServer = server.start()

  sys.addShutdownHook(server.stop())

  logger.info(s"connection information:")
  logger.info(s"  cluster: ${server.configuration.cluster}")
  logger.info(s"  id: ${server.configuration.id}")
  logger.info(s"  zookeeper connection string: ${server.configuration.zookeeperConnectionString}")
  logger.info(s"  kafka connection string: ${server.configuration.kafkaConnectionString}")
  logger.info(s"reactive database controller is started at ${server.configuration.host}:${server.configuration.port}")

  Await.ready(listeningServer)

}
