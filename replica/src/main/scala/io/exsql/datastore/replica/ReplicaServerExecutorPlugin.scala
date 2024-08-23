package io.exsql.datastore.replica

import com.twitter.finagle.ListeningServer
import com.twitter.util.Await
import com.typesafe.config.ConfigFactory
import org.apache.spark.api.plugin.{ExecutorPlugin, PluginContext}
import org.slf4j.LoggerFactory

import java.nio.file.{Path, Paths}
import java.util

class ReplicaServerExecutorPlugin extends ExecutorPlugin {

  private val logger = LoggerFactory.getLogger(classOf[ReplicaServerExecutorPlugin])

  private var listeningServer: ListeningServer = _

  private var thread: Thread = _

  private var running: Boolean = true

  override def init(ctx: PluginContext, extraConf: util.Map[String, String]): Unit = {
    listeningServer = bootstrap(Paths.get(sys.env("REPLICA_CONFIG_PATH")))
  }

  override def shutdown(): Unit = {
    running = false

    if (listeningServer != null) {
      Await.result(listeningServer.close())
    }
  }

  private def bootstrap(path: Path): ListeningServer = {
    val config = ConfigFactory
      .parseFile(path.toAbsolutePath.toFile)
      .resolve()
      .withFallback(ConfigFactory.defaultReference(Bootstrap.getClass.getClassLoader))

    logger.info("starting exsql datastore replica node service")
    logger.info(s"release information:")
    logger.info(s"  replica(${io.exsql.datastore.replica.BuildInfo})")

    val server = ReplicaServer(config)
    val listeningServer = server.start()

    sys.addShutdownHook(server.stop())

    logger.info(s"connection information:")
    logger.info(s"  cluster: ${server.configuration.cluster}")
    logger.info(s"  id: ${server.configuration.id}")
    logger.info(s"  zookeeper connection string: ${server.configuration.zookeeperConnectionString}")
    logger.info(s"  kafka connection string: ${server.configuration.kafkaConnectionString}")
    logger.info(s"replica node service is started at ${server.configuration.host}:${server.configuration.port}")

    thread = new Thread(() => {
      while (running) Await.ready(listeningServer)
    })

    thread.start()

    listeningServer
  }

}
