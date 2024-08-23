package io.exsql.datastore.replica.methods

import java.util.UUID

import io.exsql.bytegraph.ByteGraph
import io.exsql.bytegraph.metadata.ByteGraphSchema
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.replica.{NamespaceRegistry, Protocol}

object MethodReadExecutor {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodRead.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodRead.requestSchema()).map { byteGraph =>
        val namespace = byteGraph.body.namespace.as[String]()
        val stream = byteGraph.body.stream.as[String]()
        val schema = ByteGraphSchema.fromJson(ByteGraph.toJsonString(byteGraph.body.selectDynamic("schema")))

        Protocol.httpResponse(
          body = Protocol.MethodRead.response(
            requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
            values = NamespaceRegistry.get(namespace).get(stream).iterator(schema),
            valueSchema = schema,
            writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
          ),
          contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
          renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
        )
      }
  }

}
