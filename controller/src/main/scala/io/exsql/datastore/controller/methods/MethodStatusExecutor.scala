package io.exsql.datastore.controller.methods

import java.lang.management.{ManagementFactory, OperatingSystemMXBean}
import java.util.UUID

import io.exsql.datastore.controller.BuildInfo
import com.twitter.finagle.http
import com.twitter.finagle.http.Method
import com.twitter.util.Future
import io.exsql.datastore.controller.Protocol

object MethodStatusExecutor {

  private val operatingSystemMXBean: OperatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean

  def apply(request: http.Request): Future[http.Response] = request.method match {
    case Method.Options => Protocol.allowResponse(request, Protocol.MethodStatus.requestSchema())
    case _ =>
      Protocol.fromRequest(request, Protocol.MethodStatus.requestSchema()).map { byteGraph =>
        Protocol.httpResponse(
          body = Protocol.MethodStatus.response(
            requestId = byteGraph.headers.requestId.option[String]().getOrElse(UUID.randomUUID().toString),
            Array[AnyRef](
              Array[AnyRef](
                java.lang.Double.valueOf(operatingSystemMXBean.getSystemLoadAverage),
                Array[AnyRef](
                  java.lang.Long.valueOf(Runtime.getRuntime.totalMemory()),
                  java.lang.Long.valueOf(Runtime.getRuntime.freeMemory()),
                  java.lang.Long.valueOf(Runtime.getRuntime.maxMemory())
                ),
                Array[AnyRef](BuildInfo.version)
              )
            ),
            writeSchema = byteGraph.headers.showSchema.option[Boolean]().getOrElse(true),
          ),
          contentType = request.contentType.getOrElse(Protocol.BinaryContentType),
          renderAsDocument = byteGraph.headers.renderAsDocument.option[Boolean]().getOrElse(false)
        )
      }

  }

}
