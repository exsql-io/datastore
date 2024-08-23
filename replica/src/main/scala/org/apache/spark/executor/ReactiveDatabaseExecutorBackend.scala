package org.apache.spark.executor

import java.net.{URI, URL}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.deploy.worker.WorkerWatcher
import org.apache.spark.internal.Logging
import org.apache.spark.resource.ResourceProfile
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.cluster.CoarseGrainedClusterMessages.{RetrieveSparkAppConfig, SparkAppConfig}
import org.apache.spark.util.Utils
import org.apache.spark.{SecurityManager, SparkConf, SparkEnv}

import scala.collection.mutable

private[spark] object ReactiveDatabaseExecutorBackend extends Logging {

  private def run(driverUrl: String,
                  executorId: String,
                  hostname: String,
                  cores: Int,
                  appId: String,
                  workerUrl: Option[String],
                  userClassPath: Seq[URL]): Unit = {

    Utils.initDaemon(log)

    SparkHadoopUtil.get.runAsSparkUser { () =>
      // Debug code
      Utils.checkHost(hostname)

      // Bootstrap to fetch the driver's Spark properties.
      val executorConf = new SparkConf
      val fetcher = RpcEnv.create(
        "driverPropsFetcher",
        hostname,
        -1,
        executorConf,
        new SecurityManager(executorConf),
        clientMode = true)
      val driver = fetcher.setupEndpointRefByURI(driverUrl)
      val cfg = driver.askSync[SparkAppConfig](RetrieveSparkAppConfig)
      val props = cfg.sparkProperties ++ Seq[(String, String)](("spark.app.id", appId))
      fetcher.shutdown()

      // Create SparkEnv using properties we fetched from the driver.
      val driverConf = new SparkConf()
      for ((key, value) <- props) {
        // this is required for SSL in standalone mode
        if (SparkConf.isExecutorStartupConf(key)) {
          driverConf.setIfMissing(key, value)
        } else {
          driverConf.set(key, value)
        }
      }

      cfg.hadoopDelegationCreds.foreach { tokens =>
        SparkHadoopUtil.get.addDelegationTokens(tokens, driverConf)
      }

      try {
        val env = SparkEnv.createExecutorEnv(
          driverConf, executorId, hostname, cores, cfg.ioEncryptionKey, isLocal = true
        )

        env.rpcEnv.setupEndpoint("Executor", new CoarseGrainedExecutorBackend(
          env.rpcEnv, driverUrl, executorId, hostname, hostname, cores, userClassPath, env, None, ResourceProfile.getOrCreateDefaultProfile(executorConf)))
        workerUrl.foreach { url =>
          env.rpcEnv.setupEndpoint("WorkerWatcher", new WorkerWatcher(env.rpcEnv, url))
        }
        env.rpcEnv.awaitTermination()
      } catch {
        case throwable: Throwable =>
          throwable.printStackTrace()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    var driverUrl: String = null
    var executorId: String = null
    var hostname: String = null
    var cores: Int = 0
    var appId: String = null
    var workerUrl: Option[String] = None
    val userClassPath = new mutable.ListBuffer[URL]()

    var argv = args.toList
    while (!argv.isEmpty) {
      argv match {
        case ("--driver-url") :: value :: tail =>
          driverUrl = value
          argv = tail
        case ("--executor-id") :: value :: tail =>
          executorId = value
          argv = tail
        case ("--hostname") :: value :: tail =>
          hostname = value
          argv = tail
        case ("--cores") :: value :: tail =>
          cores = value.toInt
          argv = tail
        case ("--app-id") :: value :: tail =>
          appId = value
          argv = tail
        case ("--worker-url") :: value :: tail =>
          // Worker url is used in spark standalone mode to enforce fate-sharing with worker
          workerUrl = Some(value)
          argv = tail
        case ("--user-class-path") :: value :: tail =>
          userClassPath += new URI(value).toURL
          argv = tail
        case Nil =>
        case tail =>
          // scalastyle:off println
          System.err.println(s"Unrecognized options: ${tail.mkString(" ")}")
          // scalastyle:on println
          printUsageAndExit()
      }
    }

    if (driverUrl == null || executorId == null || hostname == null || cores <= 0 ||
      appId == null) {
      printUsageAndExit()
    }

    run(driverUrl, executorId, hostname, cores, appId, workerUrl, userClassPath.toSeq)
    System.exit(0)
  }

  private def printUsageAndExit(): Unit = {
    // scalastyle:off println
    System.err.println(
      """
        |Usage: ReactiveDatabaseExecutorBackend [options]
        |
        | Options are:
        |   --driver-url <driverUrl>
        |   --executor-id <executorId>
        |   --hostname <hostname>
        |   --cores <cores>
        |   --app-id <appid>
        |   --worker-url <workerUrl>
        |   --user-class-path <url>
      """.stripMargin
    )
    // scalastyle:on println
    System.exit(1)
  }

}