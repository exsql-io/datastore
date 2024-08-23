package io.exsql.datastore.replica

import org.apache.helix.HelixAdmin

import scala.jdk.CollectionConverters._

class ControllerHelper(val helixAdmin: HelixAdmin,
                       val id: String,
                       val managementCluster: String) {

  def find(): String = {
    val controllers = helixAdmin
      .getInstancesInClusterWithTag(managementCluster, "--type=controller")
      .asScala

    val controllerInstanceConfig = controllers.map { instance =>
      helixAdmin.getInstanceConfig(managementCluster, instance)
    }.head

    s"http://${controllerInstanceConfig.getHostName}:${controllerInstanceConfig.getPort}"
  }

  def sparkMaster(): String = ???

}

object ControllerHelper {

  private var instance: ControllerHelper = _

  def init(helixAdmin: HelixAdmin, id: String, managementCluster: String): Unit = {
    instance = new ControllerHelper(helixAdmin, id, managementCluster)
  }

  def apply(): ControllerHelper = instance

}
