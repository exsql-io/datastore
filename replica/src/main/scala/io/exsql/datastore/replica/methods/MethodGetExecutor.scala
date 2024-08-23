package io.exsql.datastore.replica.methods

import java.util.UUID

import io.exsql.bytegraph.ByteGraph
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.replica.{NamespaceRegistry, Protocol, SchemaRegistry}

object MethodGetExecutor {

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodGet.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodGet.requestSchema()).flatMap { byteGraph =>
        val namespace = byteGraph.body.namespace.as[String]()
        val stream = byteGraph.body.stream.as[String]()
        val schema = SchemaRegistry.getSchemaForStream(s"${namespace}.$stream")
        val partition = byteGraph.body.partition.as[Int]()

        val reader = ByteGraph.reader(schema)
        reader._partition_id = partition

        NamespaceRegistry
          .get(namespace)
          .get(stream)
          .get(byteGraph.body.key.as[String]()).map { value =>
            Protocol.httpResponse(
              body = Protocol.MethodGet.response(
                requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
                value = value.map(byteGraph => reader.read(byteGraph.bytes.bytes())),
                valueSchema = schema,
                writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true)
              ),
              contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
              renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
            )
          }
      }
  }

}
