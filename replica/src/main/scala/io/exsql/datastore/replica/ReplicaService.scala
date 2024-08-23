package io.exsql.datastore.replica

import io.exsql.datastore.replica.methods._
import com.twitter.finagle.http.{Request, Response}
import com.twitter.finagle.{Service, http}
import com.twitter.util.Future
import io.exsql.datastore.replica.methods.{MethodGetAllExecutor, MethodGetExecutor, MethodGetStreamPartitionStatisticsExecutor, MethodReadExecutor, MethodStatusExecutor}

class ReplicaService extends Service[http.Request, http.Response] {

  override def apply(request: Request): Future[Response] = request.path match {
    case Protocol.MethodStatus.Name => MethodStatusExecutor(request)
    case Protocol.MethodGet.Name => MethodGetExecutor(request)
    case Protocol.MethodGetAll.Name => MethodGetAllExecutor(request)
    case Protocol.MethodRead.Name => MethodReadExecutor(request)
    case Protocol.MethodGetStreamPartitionStatistics.Name => MethodGetStreamPartitionStatisticsExecutor(request)
  }

}

object ReplicaService {

  def apply(): ReplicaService = new ReplicaService()

}