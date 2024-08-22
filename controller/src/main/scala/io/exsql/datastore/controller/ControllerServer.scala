package io.exsql.datastore.controller

import java.net.InetSocketAddress
import ControllerServer.ControllerServerConfiguration
import com.twitter.finagle.http.Request
import com.twitter.finagle.stats.{JsonExporter, MetricsStatsReceiver}
import com.twitter.finagle.tracing.ServerTracingFilter
import com.twitter.finagle.{Http, ListeningServer, Service, http}
import com.twitter.util.Await
import com.typesafe.config.{Config, ConfigFactory}
import io.exsql.datastore.controller.kafka.{KafkaAdminHelper, KafkaConsumerHelper, KafkaProducerHelper, NamespaceHelper}
import io.exsql.datastore.controller.stm.StateMachineDefinitions
import org.apache.helix.manager.zk.ZKHelixAdmin
import org.apache.helix.model.InstanceConfig
import org.apache.helix.rest.server.HelixRestServer
import org.apache.helix.{HelixAdmin, HelixManager, HelixManagerFactory, InstanceType}

import scala.util.Try

class ControllerServer(val configuration: ControllerServerConfiguration) {

  private val metricService: Service[http.Request, http.Response] =
    new JsonExporter(MetricsStatsReceiver.defaultRegistry)

  private val kafkaAdminHelper = KafkaAdminHelper(configuration.kafkaConnectionString)

  private val kafkaProducerHelper = KafkaProducerHelper(configuration.kafkaConnectionString)

  private val kafkaConsumerHelper = KafkaConsumerHelper(configuration.id, kafkaAdminHelper)

  private val manager: HelixManager = HelixManagerFactory.getZKHelixManager(
    configuration.cluster, configuration.id, InstanceType.CONTROLLER, configuration.zookeeperConnectionString
  )

  private val namespaceStateStoreHelper = NamespaceHelper(
    kafkaAdminHelper, kafkaProducerHelper, kafkaConsumerHelper, manager, configuration.cluster
  )

  private val helixAdmin: HelixAdmin = new ZKHelixAdmin(configuration.zookeeperConnectionString)

  private val helixRestServer: Option[HelixRestServer] = {
    configuration.managementEndpointPort.map { port =>
      new HelixRestServer(configuration.zookeeperConnectionString, port, "/admin/v2")
    }
  }

  private val controllerService: Service[http.Request, http.Response] = ControllerService(
    manager, namespaceStateStoreHelper, configuration
  )

  private val instanceConfig = {
    val config = new InstanceConfig(configuration.id)
    config.setHostName(configuration.host)
    config.setPort(s"${configuration.port}")
    config.setInstanceEnabled(true)
    config.addTag("--type=controller")

    configuration.managementEndpointPort.foreach { port =>
      config.addTag(s"--management-endpoint-port=$port")
    }

    config
  }

  private val service = new Service[http.Request, http.Response] {
    override def apply(request: Request): com.twitter.util.Future[http.Response] = {
      if (request.path == "/finagle_metrics") metricService(request)
      else controllerService(request)
    }
  }

  private var server: Option[ListeningServer] = None

  def start(): ListeningServer = {
    if (!helixAdmin.getClusters.contains(configuration.cluster)) {
      helixAdmin.addCluster(configuration.cluster, true)
      helixAdmin.enableCluster(configuration.cluster, true)
      helixAdmin.addStateModelDef(
        configuration.cluster,
        StateMachineDefinitions.ReplicaDefinition.Name,
        StateMachineDefinitions.ReplicaDefinition.Definition
      )
    }

    if (!helixAdmin.getClusters.contains(configuration.managementCluster)) {
      helixAdmin.addCluster(configuration.managementCluster, true)
      helixAdmin.enableCluster(configuration.managementCluster, true)
    }

    if (!helixAdmin.getInstancesInCluster(configuration.managementCluster).contains(configuration.id)) {
      helixAdmin.addInstance(configuration.managementCluster, instanceConfig)
    }

    helixAdmin.enableInstance(configuration.managementCluster, configuration.id, true)

    manager.connect()
    helixRestServer.foreach(_.start())

    val serverBuilder = {
      Http
        .server
        .withHttp2
        .withDecompression(true)
        .withCompressionLevel(6)
        .withStreaming(true)
        .withMaxRequestSize(Protocol.DefaultMaxMessageSize)
    }

    server = Some(
      serverBuilder.serve(
        new InetSocketAddress(configuration.host, configuration.port),
        ServerTracingFilter
          .TracingFilter(label = s"controller-${configuration.host}:${configuration.port}")
          .andThen(service)
      )
    )

    server.get
  }

  def stop(): Unit = {
    helixAdmin.enableInstance(configuration.managementCluster, configuration.id, false)
    helixRestServer.foreach(_.shutdown())
    helixAdmin.close()
    manager.disconnect()
    controllerService.close()
    kafkaConsumerHelper.close()
    kafkaProducerHelper.close()
    kafkaAdminHelper.close()
    server.foreach(value => Await.ready(value.close()))
  }

}

object ControllerServer {

  case class ControllerServerConfiguration(private val configuration: Config) {
    val id: String = configuration.getString("id")
    val zookeeperConnectionString: String = configuration.getString("zookeeper-connection-string")
    val kafkaConnectionString: String = configuration.getString("kafka-connection-string")
    val host: String = configuration.getString("host")
    val port: Int = configuration.getInt("port")
    val cluster: String = configuration.getString("cluster")
    val managementCluster: String = s"$cluster-management"
    val managementEndpointPort: Option[Int] = Try(configuration.getInt("management-endpoint-port")).toOption
  }

  def apply(configuration: Config = ConfigFactory.defaultReference()): ControllerServer = {
    new ControllerServer(ControllerServerConfiguration(configuration.getConfig("controller")))
  }

}
