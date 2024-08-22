package io.exsql.datastore.controller.kafka

import io.exsql.datastore.controller.kafka.KafkaAdminHelper._
import org.apache.kafka.clients.admin._
import org.apache.kafka.common.config.TopicConfig

import scala.jdk.CollectionConverters._

class KafkaAdminHelper(val kafkaConnectionString: String) {

  private val adminClient = AdminClient.create(
    Map[String, AnyRef](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaConnectionString).asJava
  )

  def createNamespace(name: String, replicas: Short): Boolean = {
    val newTopic = new NewTopic(s"$topicPrefix-$name", 1, replicas)
    val config = new java.util.HashMap[String, String]()
    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    config.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, s"${Long.MaxValue}")
    newTopic.configs(config)

    adminClient.createTopics(Seq(newTopic).asJavaCollection).all().get()
    true
  }

  def dropNamespace(name: String): Boolean = {
    val topics = getNamespace(name).toSeq.map(_.name())
    adminClient.deleteTopics(topics.asJavaCollection).all().get()
    true
  }

  def getNamespace(name: String): Iterator[TopicListing] = {
    adminClient
      .listTopics(new ListTopicsOptions().listInternal(false))
      .listings().get().iterator().asScala.filter { topicListing =>
        topicListing.name().startsWith(s"$topicPrefix-$name")
      }
  }

  def listNamespaces(): Iterator[TopicListing] = {
    adminClient
      .listTopics(new ListTopicsOptions().listInternal(false))
      .listings().get().iterator().asScala.filter { topicListing =>
        topicListing.name().startsWith(topicPrefix)
      }
  }

  def createStream(namespace: String, name: String, partitions: Int, writeReplicas: Short): Boolean = {
    val newTopic = new NewTopic(s"$topicPrefix-$namespace.$name", partitions, writeReplicas)
    val config = new java.util.HashMap[String, String]()
    config.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT)
    config.put(TopicConfig.DELETE_RETENTION_MS_CONFIG, s"${Long.MaxValue}")
    newTopic.configs(config)

    adminClient.createTopics(Seq(newTopic).asJavaCollection).all().get()
    true
  }

  def dropStream(namespace: String, name: String): Boolean = {
    adminClient.deleteTopics(Seq(s"$topicPrefix-$namespace.$name").asJavaCollection).all().get()
    true
  }

  def close(): Unit = {
    adminClient.close()
  }

}

object KafkaAdminHelper {

  private[kafka] val topicPrefix: String = "reactive-database"

  def apply(kafkaConnectionString: String): KafkaAdminHelper = {
    new KafkaAdminHelper(kafkaConnectionString)
  }

}
