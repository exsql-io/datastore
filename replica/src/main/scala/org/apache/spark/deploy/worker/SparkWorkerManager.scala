package org.apache.spark.deploy.worker

import org.apache.spark.SparkConf
import org.apache.spark.deploy.worker.Worker.startRpcEnvAndEndpoint
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.util.{SparkUncaughtExceptionHandler, Utils}
import org.slf4j.LoggerFactory

object SparkWorkerManager {

  case class SparkWorkerConfiguration(host: String,
                                      port: Int,
                                      uiPort: Int,
                                      masterHost: String,
                                      masterPort: Int)

  case class SparkWorkerDaemonInformation(rpcEnv: RpcEnv)

  private val logger = LoggerFactory.getLogger(this.getClass)

  private var sparkWorkerDaemonInformation: SparkWorkerDaemonInformation = _

  def start(sparkWorkerConfiguration: SparkWorkerConfiguration): SparkWorkerDaemonInformation = this.synchronized {
    if (sparkWorkerDaemonInformation == null) {
      Thread.setDefaultUncaughtExceptionHandler(new SparkUncaughtExceptionHandler(exitOnUncaughtException = false))
      Utils.initDaemon(logger)

      val conf = new SparkConf
      val args = new WorkerArguments(
        Array(
          "--host", s"${sparkWorkerConfiguration.host}",
          "--port", s"${sparkWorkerConfiguration.port}",
          "--webui-port", s"${sparkWorkerConfiguration.uiPort}",
          s"spark://${sparkWorkerConfiguration.masterHost}:${sparkWorkerConfiguration.masterPort}"
        ),
        conf
      )

      val rpcEnv = startRpcEnvAndEndpoint(
        args.host, args.port, args.webUiPort, args.cores, args.memory, args.masters, args.workDir,
        conf = conf
      )

      logger.info(s"spark worker will use: host = ${rpcEnv.address.host}, port = ${rpcEnv.address.port}")

      sparkWorkerDaemonInformation = SparkWorkerDaemonInformation(rpcEnv)
    }

    sparkWorkerDaemonInformation
  }

  def stop(): Unit = this.synchronized {
    if (sparkWorkerDaemonInformation != null) {
      sparkWorkerDaemonInformation.rpcEnv.shutdown()
      sparkWorkerDaemonInformation = null
    }
  }

}
