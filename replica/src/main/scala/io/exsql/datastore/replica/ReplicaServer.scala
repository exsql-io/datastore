package io.exsql.datastore.replica

import java.net.InetSocketAddress
import java.nio.file.{Path, Paths}

import akka.actor.ActorSystem
import ReplicaServer._
import com.twitter.finagle.http.Request
import com.twitter.finagle.stats.{JsonExporter, MetricsStatsReceiver}
import com.twitter.finagle.tracing.ServerTracingFilter
import com.twitter.finagle.{Http, ListeningServer, Service, http}
import com.twitter.util.Await
import com.typesafe.config.{Config, ConfigFactory}
import io.exsql.datastore.replica.materialized.RocksDbManager
import io.exsql.datastore.replica.stm.StateMachineDefinitions
import org.apache.helix.manager.zk.ZKHelixAdmin
import org.apache.helix.model.InstanceConfig
import org.apache.helix.{HelixManagerFactory, InstanceType}
import org.slf4j.LoggerFactory

class ReplicaServer(val configuration: ReplicaServerConfiguration) {

  private val actorSystem = ActorSystem("reactive-database")

  private val helixAdmin = new ZKHelixAdmin(configuration.zookeeperConnectionString)

  private val manager = {
    val instance = HelixManagerFactory.getZKHelixManager(
      configuration.cluster, configuration.id, InstanceType.PARTICIPANT, configuration.zookeeperConnectionString
    )

    val stateMachineEngine = instance.getStateMachineEngine
    stateMachineEngine.registerStateModelFactory(
      StateMachineDefinitions.ReplicaDefinition.Name,
      StateMachineDefinitions.ReplicaDefinition.factory(configuration, actorSystem)
    )

    instance
  }

  private val instanceConfig = {
    val config = new InstanceConfig(configuration.id)
    config.setHostName(configuration.host)
    config.setPort(s"${configuration.port}")
    config.setInstanceEnabled(true)
    config.addTag("--type=replica")

    config
  }

  private val metricService: Service[http.Request, http.Response] =
    new JsonExporter(MetricsStatsReceiver.defaultRegistry)

  private val replicaService: Service[http.Request, http.Response] = ReplicaService()

  private val service = new Service[http.Request, http.Response] {
    override def apply(request: Request): com.twitter.util.Future[http.Response] = {
      if (request.path == "/finagle_metrics") metricService(request)
      else replicaService(request)
    }
  }

  private var server: Option[ListeningServer] = None

  def start(): ListeningServer = {
    logger.info("registering resources to helix")
    if (!helixAdmin.getInstancesInCluster(configuration.cluster).contains(configuration.id)) {
      helixAdmin.addInstance(configuration.cluster, instanceConfig)
    }

    logger.info("persistent storage initialization")
    RocksDbManager.init(configuration.store)

    logger.info("controller discovery initialization")
    ControllerHelper.init(helixAdmin, configuration.id, configuration.managementCluster)

    logger.info("namespace registry initialization")
    NamespaceRegistry.init(configuration.inMemory)

    logger.info("schema registry initialization")
    SchemaRegistry.init()

    logger.info("joining helix cluster")
    manager.connect()

    logger.info("enabling instance")
    helixAdmin.enableInstance(configuration.cluster, configuration.id, true)

    val serverBuilder = {
      Http
        .server
        .withHttp2
        .withDecompression(true)
        .withCompressionLevel(6)
        .withStreaming(true)
        .withMaxRequestSize(Protocol.DefaultMaxMessageSize)
    }

    logger.info("starting http server")
    server = Some(
      serverBuilder.serve(
        new InetSocketAddress(configuration.host, configuration.port),
        ServerTracingFilter
          .TracingFilter(label = s"replica-${configuration.host}:${configuration.port}")
          .andThen(service)
      )
    )

    logger.info("http server ready")
    server.get
  }

  def stop(): Unit = {
    logger.info("disconnecting from helix")
    manager.disconnect()

    logger.info("disabling instance")
    if (helixAdmin.getInstancesInCluster(configuration.cluster).contains(configuration.id)) {
      helixAdmin.enableInstance(configuration.cluster, configuration.id, false)
    }

    logger.info("closing http server")
    replicaService.close()
    server.foreach(value => Await.ready(value.close()))

    logger.info("closing helix")
    helixAdmin.close()

    logger.info("http server closed")
  }

}

object ReplicaServer {

  private val logger = LoggerFactory.getLogger(classOf[ReplicaServer])

  case class ReplicaServerConfiguration(private val configuration: Config) {
    val id: String = configuration.getString("id")
    val zookeeperConnectionString: String = configuration.getString("zookeeper-connection-string")
    val kafkaConnectionString: String = configuration.getString("kafka-connection-string")
    val host: String = configuration.getString("host")
    val port: Int = configuration.getInt("port")
    val cluster: String = configuration.getString("cluster")
    val managementCluster: String = s"$cluster-management"
    val store: Path = Paths.get(configuration.getString("store"), id)
    val inMemory: Boolean = configuration.getBoolean("in-memory")
  }

  def apply(configuration: Config = ConfigFactory.defaultReference()): ReplicaServer = {
    new ReplicaServer(ReplicaServerConfiguration(configuration.getConfig("replica")))
  }

}
