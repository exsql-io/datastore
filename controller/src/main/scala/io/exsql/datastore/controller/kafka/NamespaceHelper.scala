package io.exsql.datastore.controller.kafka

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphStructure}
import NamespaceHelper.Namespace
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol
import io.exsql.datastore.controller.stm.StateMachineDefinitions
import org.apache.helix.HelixManager

import scala.jdk.CollectionConverters._
import scala.collection.concurrent.TrieMap

class NamespaceHelper(private val kafkaAdminHelper: KafkaAdminHelper,
                      private val kafkaProducerHelper: KafkaProducerHelper,
                      private val kafkaConsumerHelper: KafkaConsumerHelper,
                      private val helixManager: HelixManager,
                      private val cluster: String) {

  private val namespaces: TrieMap[String, Namespace] = {
    val instance = TrieMap.empty[String, Namespace]
    val namespaces = kafkaAdminHelper.listNamespaces().filterNot(_.name().contains("."))
    namespaces.foreach { namespace =>
      val name = namespace.name().replace(s"${KafkaAdminHelper.topicPrefix}-", "")
      val streams = TrieMap.empty[String, NamespaceHelper.Stream]
      kafkaConsumerHelper.getStreams(name).foreach { stream => streams.put(stream.name, stream) }

      instance += name -> Namespace(name = name, replicas = 1, streams = streams)
    }

    instance
  }

  def create(name: String, replicas: Short): Future[Boolean] = Future {
    if (namespaces.contains(name)) false
    else {
      if (!kafkaAdminHelper.createNamespace(name, replicas)) false
      else {
        kafkaConsumerHelper.register(name)
        namespaces += name -> Namespace(name, replicas)
        true
      }
    }
  }

  def drop(name: String): Future[Boolean] = Future {
    if (!namespaces.contains(name)) false
    else {
      if (!kafkaAdminHelper.dropNamespace(name)) false
      else {
        namespaces -= name

        kafkaConsumerHelper.unregister(name)
        helixManager.getClusterManagmentTool.getResourcesInCluster(cluster).asScala.foreach { resource =>
          if (resource.startsWith(s"$name.")) {
            helixManager.getClusterManagmentTool.dropResource(cluster, resource)
          }
        }

        true
      }
    }
  }

  def get(name: String): Future[Option[Namespace]] = Future {
    namespaces.get(name)
  }

  def list(): Future[Iterator[(String, Namespace)]] = Future {
    namespaces.iterator
  }

  def createStream(stream: ByteGraph): Future[Boolean] = {
    val namespace = stream.namespace.as[String]()
    val name = stream.name.as[String]()

    if (!namespaces.contains(namespace)) Future.False
    else {
      if (namespaces(namespace).streams.contains(name)) Future.False
      else {
        kafkaProducerHelper.createStream(stream).foreach { result =>
          if (result) {
            val schema = stream.selectDynamic("schema").as[ByteGraphStructure]()
            val partitions = stream.partitions.as[Int]()
            val writeReplicas = stream.writeReplicas.as[Short]()
            val deployReplicas = stream.deployReplicas.option[Short]()

            val instance = namespaces(namespace)
            instance.streams.put(
              name,
              NamespaceHelper.Stream(name, schema, partitions, writeReplicas, deployReplicas, placement = None)
            )

            if (kafkaAdminHelper.createStream(namespace, name, partitions, writeReplicas)) {
              helixManager.getClusterManagmentTool.addResource(
                cluster,
                s"$namespace.$name",
                partitions,
                StateMachineDefinitions.ReplicaDefinition.Name,
                StateMachineDefinitions.ReplicaDefinition.AssignmentStrategyName
              )

              if (deployReplicas.isDefined) helixManager.getClusterManagmentTool.rebalance(
                cluster, s"$namespace.$name", deployReplicas.get.toInt
              )
            }
          }
        }
      }
    }
  }

  def dropStream(namespace: String, name: String): Future[Boolean] = {
    if (!namespaces.contains(namespace)) Future.False
    else {
      if (!namespaces(namespace).streams.contains(name)) Future.False
      else {
        kafkaProducerHelper.dropStream(namespace, name).foreach { result =>
          if (result) {
            if (kafkaAdminHelper.dropStream(namespace, name)) {
              helixManager.getClusterManagmentTool.dropResource(cluster, s"$namespace.$name")
              namespaces(namespace).streams.remove(name)
            }
          }
        }
      }
    }
  }

  def getStream(namespace: String, name: String): Future[Option[NamespaceHelper.Stream]] = Future {
    namespaces.get(namespace).flatMap(_.streams.get(name))
  }

  def deployStream(stream: ByteGraph): Future[Boolean] = {
    val namespace = stream.namespace.as[String]()
    val name = stream.name.as[String]()
    val deployReplicas = stream.deployReplicas.as[Short]()

    if (!namespaces.contains(namespace)) Future.False
    else {
      if (!namespaces(namespace).streams.contains(name)) Future.False
      else {
        kafkaProducerHelper.deployStream(namespace, name, deployReplicas).map { success =>
          if (success) {
            val stream = namespaces(namespace).streams(name)
            namespaces(namespace).streams.replace(
              name,
              stream.copy(deployReplicas = Some(deployReplicas))
            )

            helixManager.getClusterManagmentTool.dropResource(cluster, s"$namespace.$name")
            helixManager.getClusterManagmentTool.addResource(
              cluster,
              s"${namespace}.$name",
              stream.partitions,
              StateMachineDefinitions.ReplicaDefinition.Name,
              StateMachineDefinitions.ReplicaDefinition.AssignmentStrategyName
            )

            helixManager.getClusterManagmentTool.rebalance(cluster, s"$namespace.$name", deployReplicas.toInt)
          }

          success
        }
      }
    }
  }

  def placement(namespace: String, stream: NamespaceHelper.Stream): Future[NamespaceHelper.Stream] = Future {
    val externalView = helixManager
      .getClusterManagmentTool
      .getResourceExternalView(cluster, s"$namespace.${stream.name}")

    val byteGraphStructureBuilder = ByteGraph.structureBuilder()
    externalView.getPartitionSet.asScala.foreach { partition =>
      byteGraphStructureBuilder.appendStructure(partition.split('_')(1)) { partitionStateBuilder =>
        externalView.getStateMap(partition).asScala.foreach { case (instanceId, state) =>
          val instanceConfig = helixManager.getClusterManagmentTool.getInstanceConfig(cluster, instanceId)
          partitionStateBuilder.appendString(state, s"${instanceConfig.getHostName}:${instanceConfig.getPort}")
        }
      }
    }

    stream.copy(placement = Some(byteGraphStructureBuilder.build()))
  }

}

object NamespaceHelper {

  private val namespaceWriter = ByteGraph.writer(Protocol.NamespaceDefinition)

  case class Stream(name: String,
                    schema: ByteGraphStructure,
                    partitions: Int,
                    writeReplicas: Short,
                    deployReplicas: Option[Short],
                    placement: Option[ByteGraphStructure]) {

    def row(namespace: String): Array[AnyRef] = {
      Array[AnyRef](
        namespace,
        name,
        schema,
        java.lang.Integer.valueOf(partitions),
        java.lang.Short.valueOf(writeReplicas),
        deployReplicas.map(java.lang.Short.valueOf).orNull,
        placement.orNull
      )
    }

  }

  object Stream {

    def fromByteGraph(byteGraph: ByteGraph): Stream = {
      Stream(
        name = byteGraph.name.as[String](),
        schema = byteGraph.selectDynamic("schema").as[ByteGraphStructure](),
        partitions = byteGraph.partitions.as[Int](),
        writeReplicas = byteGraph.`writeReplicas`.as[Short](),
        deployReplicas = byteGraph.`deployReplicas`.option[Short](),
        placement = None
      )
    }

  }

  case class Namespace(name: String, replicas: Short, streams: TrieMap[String, Stream] = TrieMap.empty) {

    lazy val byteGraph: ByteGraph = ByteGraph.valueOf(
      namespaceWriter.write(row(), true),
      Some(Protocol.NamespaceDefinition)
    )

    def row(): Array[AnyRef] = {
      Array[AnyRef](name, java.lang.Short.valueOf(replicas), streams.values.map(_.row(name)).toArray)
    }

  }

  def apply(kafkaAdminHelper: KafkaAdminHelper,
            kafkaProducerHelper: KafkaProducerHelper,
            kafkaConsumerHelper: KafkaConsumerHelper,
            helixManager: HelixManager,
            cluster: String): NamespaceHelper = {

    new NamespaceHelper(kafkaAdminHelper, kafkaProducerHelper, kafkaConsumerHelper, helixManager, cluster)
  }

}
