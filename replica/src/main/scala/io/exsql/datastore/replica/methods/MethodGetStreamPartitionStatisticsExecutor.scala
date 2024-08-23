package io.exsql.datastore.replica.methods

import java.util.UUID
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.replica.{NamespaceRegistry, Protocol}

object MethodGetStreamPartitionStatisticsExecutor {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodGetStreamPartitionStatistics.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodGetStreamPartitionStatistics.requestSchema()).map { byteGraph =>
        val namespace = byteGraph.body.namespace.as[String]()
        val stream = byteGraph.body.stream.as[String]()
        val partition = byteGraph.body.partition.as[Int]()
        val statistics = getStatistics(namespace, stream, partition)

        Protocol.httpResponse(
          body = Protocol.MethodGetStreamPartitionStatistics.response(
            requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
            namespace = namespace,
            stream = stream,
            partition = partition,
            statistics = statistics,
            writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
          ),
          contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
          renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
        )
      }
  }

  private def getStatistics(namespace: String, stream: String, partition: Int): Array[AnyRef] = {
    Array[AnyRef](
      java.lang.Long.valueOf(NamespaceRegistry.get(namespace).get(stream).count(partition)),
      java.lang.Long.valueOf(NamespaceRegistry.get(namespace).get(stream).bytes(partition))
    )
  }

}
