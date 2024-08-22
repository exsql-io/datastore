package io.exsql.bytegraph.ipc

import java.sql.{ResultSet, ResultSetMetaData}

import io.exsql.bytegraph.ByteGraph.ByteGraphRow
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphDataType, ByteGraphListType, ByteGraphRowType, ByteGraphSchemaField}
import io.exsql.bytegraph.{ByteGraph, ByteGraphResultSetWriter, ByteGraphValueType, ByteGraphWriter}
import io.exsql.bytegraph.builder.ByteGraphRowBuilder
import io.exsql.bytegraph.bytes.ByteGraphBytes
import io.exsql.bytegraph.metadata.ByteGraphSchema

trait ByteGraphMessageBuilder {
  val headersDefinition: ByteGraphRowType
  val bodyDefinition: ByteGraphRowType
  val schema: ByteGraphRowType

  protected lazy val headerWriter: ByteGraphWriter = ByteGraph.writer(headersDefinition)

  protected def createBuilder(headers: Array[AnyRef], writeSchema: Boolean): ByteGraphRowBuilder = {
    val builder = ByteGraph.rowBuilder(schema)
    if (writeSchema) builder.append(schema.byteGraph.bytes.slice(1))
    else builder.appendNull()

    builder.append(headerWriter.write(headers, false))
    builder
  }
}

class ByteGraphSingleMessageBuilder(override val headersDefinition: ByteGraphRowType,
                                    override val bodyDefinition: ByteGraphRowType) extends ByteGraphMessageBuilder {

  override val schema: ByteGraphRowType = ByteGraphMessageBuilder.messageSchemaFrom(headersDefinition, bodyDefinition)

  lazy val bodyWriter: ByteGraphWriter = ByteGraph.writer(bodyDefinition)

  def build(headers: Array[AnyRef], row: Option[Array[AnyRef]], writeSchema: Boolean = true): ByteGraphRow = {
    buildFromByteGraphBytes(headers, row.map(bodyWriter.write(_, false)), writeSchema)
  }

  def buildFromByteGraphBytes(headers: Array[AnyRef], row: Option[ByteGraphBytes], writeSchema: Boolean): ByteGraphRow = {
    val builder = createBuilder(headers, writeSchema)
    if (row.isEmpty) builder.appendNull().build()
    else builder.append(row.get).build()
  }

}

class ByteGraphResultSetMessageBuilder(override val headersDefinition: ByteGraphRowType,
                                       override val bodyDefinition: ByteGraphRowType,
                                       private val resultSetMetaData: ResultSetMetaData) extends ByteGraphMessageBuilder {

  val bodyWriter: ByteGraphResultSetWriter = ByteGraph.resultSetWriter(bodyDefinition, resultSetMetaData)

  override val schema: ByteGraphRowType = ByteGraphMessageBuilder.messageSchemaFrom(
    headersDefinition, ByteGraphListType(bodyWriter.schema)
  )

  def build(headers: Array[AnyRef], resultSet: ResultSet, writeSchema: Boolean = true): ByteGraphRow = {
    createBuilder(headers, writeSchema)
      .appendList { listBuilder =>
        while (resultSet.next()) {
          listBuilder.append(bodyWriter.write(resultSet, true))
        }
      }
      .build()
  }

}

class ByteGraphIteratorMessageBuilder(override val headersDefinition: ByteGraphRowType,
                                      override val bodyDefinition: ByteGraphRowType) extends ByteGraphMessageBuilder {

  override val schema: ByteGraphRowType = ByteGraphMessageBuilder.messageSchemaFrom(
    headersDefinition, ByteGraphListType(bodyDefinition)
  )

  lazy val bodyWriter: ByteGraphWriter = ByteGraph.writer(bodyDefinition)

  def build(headers: Array[AnyRef], rows: Iterator[Array[AnyRef]], writeSchema: Boolean = true): ByteGraphRow = {
    buildFromByteGraphBytes(headers, rows.map(bodyWriter.write(_, true)), writeSchema)
  }

  def buildFromByteGraphBytes(headers: Array[AnyRef], rows: Iterator[ByteGraphBytes], writeSchema: Boolean): ByteGraphRow = {
    createBuilder(headers, writeSchema)
      .appendList { listBuilder =>
        while (rows.hasNext) {
          val row = rows.next()
          if (row == null) listBuilder.appendNull()
          else listBuilder.append(row)
        }
      }
      .build()
  }

}

object ByteGraphMessageBuilder {

  def forResultSet(headersDefinition: ByteGraphRowType,
                   bodyDefinition: ByteGraphRowType,
                   resultSetMetaData: ResultSetMetaData): ByteGraphResultSetMessageBuilder = {

    new ByteGraphResultSetMessageBuilder(headersDefinition, bodyDefinition, resultSetMetaData)
  }

  def forRows(headersDefinition: ByteGraphRowType,
              bodyDefinition: ByteGraphRowType): ByteGraphIteratorMessageBuilder = {

    new ByteGraphIteratorMessageBuilder(headersDefinition, bodyDefinition)
  }

  def forRow(headersDefinition: ByteGraphRowType,
             bodyDefinition: ByteGraphRowType): ByteGraphSingleMessageBuilder = {

    new ByteGraphSingleMessageBuilder(headersDefinition, bodyDefinition)
  }

  private[ipc] def messageSchemaFrom(headersDefinition: ByteGraphRowType,
                                     bodyDefinition: ByteGraphDataType): ByteGraphRowType = {

    ByteGraphSchema.rowOf(
      ByteGraphSchemaField("schema", ByteGraphValueType.Structure),
      ByteGraphSchemaField("headers", headersDefinition),
      ByteGraphSchemaField("body", bodyDefinition)
    )
  }

}
