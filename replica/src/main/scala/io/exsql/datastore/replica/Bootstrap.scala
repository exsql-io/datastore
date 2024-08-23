package io.exsql.datastore.replica

import java.nio.file.{Path, Paths}

import com.twitter.util.{Await, Future}
import com.typesafe.config.ConfigFactory
import org.apache.spark.deploy.worker.SparkWorkerManager
import org.apache.spark.deploy.worker.SparkWorkerManager.SparkWorkerConfiguration
import org.slf4j.LoggerFactory

object Bootstrap extends App {

  private val logger = LoggerFactory.getLogger("io.exsql.datastore.replica.Bootstrap")

  private val configurationPath = sys.env("REPLICA_CONFIG_PATH")

  Await.ready(bootstrap(Paths.get(configurationPath)))

  private def bootstrap(path: Path): Future[Unit] = {
    val config = ConfigFactory
      .parseFile(path.toAbsolutePath.toFile)
      .resolve()
      .withFallback(ConfigFactory.defaultReference(Bootstrap.getClass.getClassLoader))

    logger.info("starting reactive database spark worker node")

    val sparkWorkerConfiguration = SparkWorkerConfiguration(
      host = config.getString("replica.host"),
      port = config.getInt("replica.spark-worker.port"),
      uiPort = config.getInt("replica.spark-worker.ui-port"),
      masterHost = config.getString("replica.spark-worker.master.host"),
      masterPort = config.getInt("replica.spark-worker.master.port")
    )

    logger.info(s"  internal spark worker will be started at ${sparkWorkerConfiguration.host}:${sparkWorkerConfiguration.port}")
    logger.info(s"  internal spark worker will use master at ${sparkWorkerConfiguration.masterHost}:${sparkWorkerConfiguration.masterPort}")

    val sparkWorkerDaemonInformation = SparkWorkerManager.start(sparkWorkerConfiguration)
    sys.addShutdownHook(SparkWorkerManager.stop())

    logger.info(s"reactive database spark worker node is started")

    Future.value(sparkWorkerDaemonInformation.rpcEnv.awaitTermination())
  }

}
