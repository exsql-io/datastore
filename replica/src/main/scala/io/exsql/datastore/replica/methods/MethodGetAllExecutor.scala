package io.exsql.datastore.replica.methods

import java.util.UUID

import io.exsql.bytegraph.ByteGraph.ByteGraphList
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.replica.{NamespaceRegistry, Protocol}

object MethodGetAllExecutor {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodGetAll.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodGetAll.requestSchema()).flatMap { byteGraph =>
        val namespace = byteGraph.body.namespace.as[String]()
        val streamName = byteGraph.body.stream.as[String]()

        val stream = NamespaceRegistry
          .get(namespace)
          .get(streamName)

        stream.getAll(byteGraph.body.keys.as[ByteGraphList]().iterator.map(_.as[String]())).toSeq().map { values =>
          Protocol.httpResponse(
            body = Protocol.MethodGetAll.response(
              requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
              values = values.iterator,
              valueSchema = stream.schema,
              writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
            ),
            contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
            renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
          )
        }
      }
  }

}
