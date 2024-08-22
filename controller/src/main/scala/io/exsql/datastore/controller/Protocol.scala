package io.exsql.datastore.controller

import java.time.{Duration, LocalDate, ZonedDateTime}
import java.util.Base64
import io.exsql.bytegraph.ByteGraph.{ByteGraph, ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.bytes.{ByteArrayByteGraphBytes, ByteGraphBytes}
import io.exsql.bytegraph.ipc.ByteGraphMessageBuilder
import io.exsql.bytegraph.metadata.ByteGraphSchema
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphRowType, ByteGraphSchema, ByteGraphSchemaField, ByteGraphStructureType}
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import com.twitter.finagle.http
import com.twitter.io.{Buf, Reader}
import com.twitter.util.{Future, StorageUnit}

import scala.collection.compat.immutable.ArraySeq

object Protocol {

  val BinaryContentType: String = "application/bytegraph"

  val TextContentType: String = "application/json"

  val DefaultMaxMessageSize: StorageUnit = StorageUnit.fromMegabytes(1024)

  val StreamDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("namespace", ByteGraphValueType.String),
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("schema", ByteGraphValueType.Structure),
    ByteGraphSchemaField("partitions", ByteGraphValueType.Int),
    ByteGraphSchemaField("writeReplicas", ByteGraphValueType.Short),
    ByteGraphSchemaField("deployReplicas", ByteGraphValueType.Short)
  )

  val NamespaceDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("replicas", ByteGraphValueType.Short),
    ByteGraphSchemaField("streams", ByteGraphSchema.listOf(StreamDefinition))
  )

  val ClusterDefinition: ByteGraphRowType = ByteGraphSchema.rowOf(
    ByteGraphSchemaField("name", ByteGraphValueType.String),
    ByteGraphSchemaField("controllers", ByteGraphSchema.listOf(ByteGraphSchema.rowOf(
      ByteGraphSchemaField("id", ByteGraphValueType.String),
      ByteGraphSchemaField("host", ByteGraphValueType.String),
      ByteGraphSchemaField("port", ByteGraphValueType.Int),
      ByteGraphSchemaField("managementPort", ByteGraphValueType.Int)
    ))),
    ByteGraphSchemaField("zookeeper-connection-string", ByteGraphValueType.String),
    ByteGraphSchemaField("kafka-connection-string", ByteGraphValueType.String)
  )

  private val serviceName: String = "io.exsql.datastore.controller.ReactiveDatabaseControllerService"

  private val requestIdHeaderDefinition = ByteGraphSchemaField("requestId", ByteGraphValueType.String)

  private val showSchemaHeaderDefinition = ByteGraphSchemaField("showSchema", ByteGraphValueType.Boolean)

  private val renderAsDocumentHeaderDefinition = ByteGraphSchemaField("renderAsDocument", ByteGraphValueType.Boolean)

  private val allowResponseMessageBuilder = ByteGraphMessageBuilder.forRow(
    headersDefinition = ByteGraphSchema.rowOf(
      requestIdHeaderDefinition, showSchemaHeaderDefinition, renderAsDocumentHeaderDefinition
    ),
    bodyDefinition = ByteGraphSchema.rowOf()
  )

  object MethodStatus {

    val Name: String = s"/$serviceName/Status"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf()
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("serverInfo", ByteGraphSchema.rowOf(
          ByteGraphSchemaField("loadAverage", ByteGraphValueType.Double),
          ByteGraphSchemaField("memoryInfo", ByteGraphSchema.rowOf(
            ByteGraphSchemaField("used", ByteGraphValueType.Long),
            ByteGraphSchemaField("free", ByteGraphValueType.Long),
            ByteGraphSchemaField("max", ByteGraphValueType.Long)
          )),
          ByteGraphSchemaField("buildInfo", ByteGraphSchema.rowOf(
            ByteGraphSchemaField("version", ByteGraphValueType.String)
          ))
        ))
      )
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), None, writeSchema)
    }

    def response(requestId: String, serverInfo: Array[AnyRef], writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(serverInfo), writeSchema)
    }

  }

  object MethodCreateNamespace {

    val Name: String = s"/$serviceName/CreateNamespace"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("name", ByteGraphValueType.String),
        ByteGraphSchemaField("replicas", ByteGraphValueType.Short)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, name: String, replicas: Short, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), Some(Array(name, java.lang.Short.valueOf(replicas))), writeSchema)
    }

    def response(requestId: String, success: Boolean, writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(Array[AnyRef](java.lang.Boolean.valueOf(success))), writeSchema)
    }

  }

  object MethodDropNamespace {

    val Name: String = s"/$serviceName/DropNamespace"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("name", ByteGraphValueType.String))
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, name: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), Some(Array(name)), writeSchema)
    }

    def response(requestId: String, success: Boolean, writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(Array[AnyRef](java.lang.Boolean.valueOf(success))), writeSchema)
    }

  }

  object MethodGetNamespace {

    val Name: String = s"/$serviceName/GetNamespace"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("name", ByteGraphValueType.String))
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = NamespaceDefinition
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, name: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), Some(Array(name)), writeSchema)
    }

    def response(requestId: String, namespace: Option[ByteGraphBytes], writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.buildFromByteGraphBytes(Array(requestId), namespace, writeSchema)
    }

  }

  object MethodListNamespaces {

    val Name: String = s"/$serviceName/ListNamespaces"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf()
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRows(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = NamespaceDefinition
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), None, writeSchema)
    }

    def response(requestId: String, namespaces: Iterator[ByteGraphBytes], writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.buildFromByteGraphBytes(Array(requestId), namespaces, writeSchema)
    }

  }

  object MethodCreateStream {

    val Name: String = s"/$serviceName/CreateStream"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = StreamDefinition
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String,
                namespace: String,
                name: String,
                schema: ByteGraphStructure,
                writeReplicas: Short,
                deployReplicas: Option[Short],
                writeSchema: Boolean): ByteGraph = {

      val row = Array[AnyRef](
        namespace, name, schema, java.lang.Short.valueOf(writeReplicas), deployReplicas.map(java.lang.Short.valueOf).orNull
      )

      requestMessageBuilder.build(Array(requestId), Some(row), writeSchema)
    }

    def response(requestId: String, success: Boolean, writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(Array[AnyRef](java.lang.Boolean.valueOf(success))), writeSchema)
    }

  }

  object MethodDropStream {

    val Name: String = s"/$serviceName/DropStream"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, namespace: String, name: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), Some(Array(namespace, name)), writeSchema)
    }

    def response(requestId: String, success: Boolean, writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(Array[AnyRef](java.lang.Boolean.valueOf(success))), writeSchema)
    }

  }

  object MethodGetStream {

    val Name: String = s"/$serviceName/GetStream"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String),
        ByteGraphSchemaField("withPlacement", ByteGraphValueType.Boolean)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ArraySeq.unsafeWrapArray(StreamDefinition.fields) :+ ByteGraphSchemaField("withPlacement", ByteGraphValueType.Boolean): _*
      )
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String,
                namespace: String,
                name: String,
                withPlacement: Boolean,
                writeSchema: Boolean): ByteGraph = {

      requestMessageBuilder.build(
        Array(requestId),
        Some(Array(namespace, name, java.lang.Boolean.valueOf(withPlacement))),
        writeSchema
      )
    }

    def response(requestId: String, stream: Option[Array[AnyRef]], writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), stream, writeSchema)
    }

  }

  object MethodDeployStream {

    val Name: String = s"/$serviceName/DeployStream"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(
        ByteGraphSchemaField("namespace", ByteGraphValueType.String),
        ByteGraphSchemaField("name", ByteGraphValueType.String),
        ByteGraphSchemaField("deployReplicas", ByteGraphValueType.Short)
      )
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf(ByteGraphSchemaField("success", ByteGraphValueType.Boolean))
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, namespace: String, name: String, deployReplicas: Short, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(
        Array(requestId),
        Some(Array(namespace, name, java.lang.Short.valueOf(deployReplicas))),
        writeSchema
      )
    }

    def response(requestId: String, success: Boolean, writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(Array[AnyRef](java.lang.Boolean.valueOf(success))), writeSchema)
    }

  }

  object MethodDescribeCluster {

    val Name: String = s"/$serviceName/DescribeCluster"

    private val requestMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ByteGraphSchema.rowOf()
    )

    private val responseMessageBuilder = ByteGraphMessageBuilder.forRow(
      headersDefinition = ByteGraphSchema.rowOf(requestIdHeaderDefinition),
      bodyDefinition = ClusterDefinition
    )

    def requestSchema(): ByteGraphSchema = requestMessageBuilder.schema

    def responseSchema(): ByteGraphSchema = responseMessageBuilder.schema

    def request(requestId: String, writeSchema: Boolean): ByteGraph = {
      requestMessageBuilder.build(Array(requestId), None, writeSchema)
    }

    def response(requestId: String, cluster: Array[AnyRef], writeSchema: Boolean): ByteGraph = {
      responseMessageBuilder.build(Array(requestId), Some(cluster), writeSchema)
    }

  }

  def allowResponse(request: http.Request, schema: ByteGraphSchema): Future[http.Response] = {
    fromRequest(request, schema).map { byteGraph =>
      val requestId = byteGraph.requestId.option[String]()
      val contentType: String = request.contentType.getOrElse(BinaryContentType)
      allowResponse(requestId, contentType)
    }
  }

  private def allowResponse(requestId: Option[String], contentType: String): http.Response = {
    val response = httpResponse(allowResponseMessageBuilder.build(Array(requestId.orNull), None), contentType)
    response
      .headerMap
      .add(com.twitter.finagle.http.Fields.AccessControlAllowMethods, s"${com.twitter.finagle.http.Method.Post.name}, ${com.twitter.finagle.http.Method.Options.name}")
      .add(
        com.twitter.finagle.http.Fields.AccessControlAllowHeaders,
        s"${com.twitter.finagle.http.Fields.ContentType}, ${com.twitter.finagle.http.Fields.Accept}, ${com.twitter.finagle.http.Fields.Origin}, ${com.twitter.finagle.http.Fields.AccessControlAllowOrigin}, X-Requested-With"
      )

    response
  }

  def httpResponse(body: ByteGraph, contentType: String, renderAsDocument: Boolean = false): http.Response = {
    val justContentType = contentType.replace(";charset=utf-8", "")

    val response = com.twitter.finagle.http.Response(com.twitter.finagle.http.Status.Ok)

    val content = toBuf(body, justContentType, renderAsDocument)
    if (!content.isEmpty) response.content(content)
    response.setContentType(justContentType)
    response.headerMap.add(com.twitter.finagle.http.Fields.AccessControlAllowOrigin, "*")

    response
  }

  def fromRequest(request: http.Request, schema: ByteGraphSchema): Future[ByteGraph] = {
    request.method match {
      case http.Method.Get => fromGetRequest(request, schema)
      case _ =>
        val stream = Reader.toAsyncStream(request.reader)
        stream.toSeq().map(Buf(_)).map { buf =>
          request.contentType match {
            case Some(contentType) if contentType.contains(TextContentType) =>
              ByteGraph.fromJson(
                new String(fromBuf(buf), "UTF-8"),
                Some(convertToByteGraphStructureType(schema))
              ).as[ByteGraphStructure]()

            case _ => ByteGraph.valueOf(ByteArrayByteGraphBytes(fromBuf(buf)), Some(schema)).as[ByteGraphRow]()
          }
        }
    }
  }

  private def fromBuf(buf: Buf): Array[Byte] = {
    Buf.ByteArray.Owned.extract(buf)
  }

  private def toBuf(byteGraph: ByteGraph, contentType: String, renderAsDocument: Boolean): Buf = {
    val bytes: Array[Byte] = {
      if (contentType.startsWith(TextContentType)) ByteGraph.toJson(byteGraph, renderAsDocument)
      else ByteGraph.render(byteGraph, withVersionFlag = true).bytes()
    }

    Buf.ByteArray.Owned(bytes)
  }

  private def fromGetRequest(request: http.Request, schema: ByteGraphSchema): Future[ByteGraph] = Future {
    val builder = ByteGraph.rowBuilder(schema.asInstanceOf[ByteGraphRowType])
    schema.fields.foreach { field =>
      if (!request.containsParam(field.name)) builder.appendNull()
      else {
        (field.byteGraphDataType.valueType: @unchecked) match {
          case ByteGraphValueType.Null => builder.appendNull()
          case ByteGraphValueType.Boolean => builder.appendBoolean(request.getBooleanParam(field.name))
          case ByteGraphValueType.Short => builder.appendShort(request.getShortParam(field.name))
          case ByteGraphValueType.Int => builder.appendInt(request.getIntParam(field.name))
          case ByteGraphValueType.Long => builder.appendLong(request.getLongParam(field.name))
          case ByteGraphValueType.UShort => builder.appendUShort(request.getShortParam(field.name))
          case ByteGraphValueType.UInt => builder.appendUInt(request.getIntParam(field.name))
          case ByteGraphValueType.ULong => builder.appendULong(request.getLongParam(field.name))
          case ByteGraphValueType.Float => builder.appendFloat(request.getParam(field.name).toFloat)
          case ByteGraphValueType.Double => builder.appendDouble(request.getParam(field.name).toDouble)
          case ByteGraphValueType.Date => builder.appendDate(LocalDate.parse(request.getParam(field.name)))
          case ByteGraphValueType.DateTime => builder.appendDateTime(ZonedDateTime.parse(request.getParam(field.name)))
          case ByteGraphValueType.Duration => builder.appendDuration(Duration.parse(request.getParam(field.name)))
          case ByteGraphValueType.String => builder.appendString(request.getParam(field.name))
          case ByteGraphValueType.Blob => builder.appendBlob(Base64.getDecoder.decode(request.getParam(field.name)))
          case ByteGraphValueType.List => ByteGraph.fromJson(request.getParam(field.name)).as[ByteGraphList]()
          case ByteGraphValueType.Structure =>
            ByteGraph
              .fromJson(request.getParam(field.name), Some(field.byteGraphDataType.asInstanceOf[ByteGraphStructureType]))
              .as[ByteGraphStructure]()

          case ByteGraphValueType.Row =>
            ByteGraph
              .fromJson(request.getParam(field.name), Some(field.byteGraphDataType.asInstanceOf[ByteGraphRowType]))
              .as[ByteGraphRowType]()

          case ByteGraphValueType.Symbol => ???
          case ByteGraphValueType.Reference => ???
        }
      }
    }

    builder.build()
  }

  private def convertToByteGraphStructureType(byteGraphSchema: ByteGraphSchema): ByteGraphStructureType = {
    byteGraphSchema match {
      case structureType: ByteGraphStructureType => structureType
      case _ =>
        ByteGraphSchema.structureOf(ArraySeq.unsafeWrapArray(byteGraphSchema.fields).map { field =>
          field.byteGraphDataType match {
            case byteGraphRowType: ByteGraphRowType =>
              field.copy(byteGraphDataType = convertToByteGraphStructureType(byteGraphRowType))

            case _ => field
          }
        }: _*)
    }
  }

}
