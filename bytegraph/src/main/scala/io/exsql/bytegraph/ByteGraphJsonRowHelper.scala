package io.exsql.bytegraph

import com.jsoniter.ValueType
import ByteGraph.ByteGraph
import ByteGraphJsonHelper.buildStructureFromEntryIterator
import io.exsql.bytegraph.builder.{ByteGraphListBuilder, ByteGraphRowBuilder}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphDataType, ByteGraphListType, ByteGraphRowType, ByteGraphSchemaField, ByteGraphStructureType}

import java.time.{Duration, LocalDate, ZonedDateTime}
import java.util.Base64
import scala.jdk.CollectionConverters._

private[bytegraph] object ByteGraphJsonRowHelper {

  def fromJsonRow(any: com.jsoniter.any.Any, schema: ByteGraphRowType): ByteGraph = {
    if (any.valueType() == ValueType.ARRAY) fromListOfEntries(any.asList().asScala.toSeq, schema)
    else fromMap(any.asMap(), schema)
  }

  private def fromMap(map: java.util.Map[String, com.jsoniter.any.Any], schema: ByteGraphRowType): ByteGraph = {
    val byteGraphRowBuilder = ByteGraph.rowBuilder(schema)
    val entries: Seq[com.jsoniter.any.Any] = schema.fields.map(field => map.get(field.name)).toSeq
    buildForSchemaRowFromEntries(entries, byteGraphRowBuilder, schema)
    byteGraphRowBuilder.build()
  }

  private def fromListOfEntries(entries: Seq[com.jsoniter.any.Any], schema: ByteGraphRowType): ByteGraph = {
    val byteGraphRowBuilder = ByteGraph.rowBuilder(schema)
    buildForSchemaRowFromEntries(entries, byteGraphRowBuilder, schema)
    byteGraphRowBuilder.build()
  }

  private def buildForSchemaRowFromEntries(entries: Seq[com.jsoniter.any.Any],
                                           byteGraphRowBuilder: ByteGraphRowBuilder,
                                           schema: ByteGraphRowType): Unit = {

    var index = 0
    val iterator = entries.iterator
    while (iterator.hasNext) {
      val entry = iterator.next()
      fieldTypeFromFieldWithSchema(entry, schema.fields(index), byteGraphRowBuilder)
      index += 1
    }
  }

  private def fieldTypeFromFieldWithSchema(value: com.jsoniter.any.Any,
                                           fieldSchema: ByteGraphSchemaField,
                                           byteGraphRowBuilder: ByteGraphRowBuilder): Unit = {

    if (value == null || value.valueType() == ValueType.NULL) byteGraphRowBuilder.appendNull()
    else {
      fieldSchema.byteGraphDataType.valueType match {
        case ByteGraphValueType.Boolean => byteGraphRowBuilder.appendBoolean(value.toBoolean)
        case ByteGraphValueType.Short => byteGraphRowBuilder.appendShort(value.toInt.toShort)
        case ByteGraphValueType.Int => byteGraphRowBuilder.appendInt(value.toInt)
        case ByteGraphValueType.Long => byteGraphRowBuilder.appendLong(value.toLong)
        case ByteGraphValueType.Float => byteGraphRowBuilder.appendFloat(value.toFloat)
        case ByteGraphValueType.Double => byteGraphRowBuilder.appendDouble(value.toDouble)
        case ByteGraphValueType.Symbol => byteGraphRowBuilder.appendString(value.as(classOf[String]))
        case ByteGraphValueType.String => byteGraphRowBuilder.appendString(value.as(classOf[String]))
        case ByteGraphValueType.Date =>
          byteGraphRowBuilder.appendDate(LocalDate.parse(value.as(classOf[String])))

        case ByteGraphValueType.DateTime =>
          byteGraphRowBuilder.appendDateTime(ZonedDateTime.parse(value.as(classOf[String])))

        case ByteGraphValueType.Duration =>
          byteGraphRowBuilder.appendDuration(Duration.parse(value.as(classOf[String])))

        case ByteGraphValueType.Blob =>
          byteGraphRowBuilder.appendBlob(Base64.getDecoder.decode(value.as(classOf[String])))

        case ByteGraphValueType.List =>
          byteGraphRowBuilder.appendList { listBuilder =>
            fieldSchema.byteGraphDataType match {
              case listType: ByteGraphListType => buildForSchemaListFromListOfAny(value.asList(), listType, listBuilder)
              case _ => ByteGraphJsonHelper.buildListFromListOfAny(value.asList(), listBuilder, None)
            }
          }

        case ByteGraphValueType.Structure =>
          byteGraphRowBuilder.appendStructure { structureBuilder =>
            val structureSchema: Option[ByteGraphStructureType] = {
              fieldSchema.byteGraphDataType match {
                case structureType: ByteGraphStructureType => Some(structureType)
                case _ => None
              }
            }

            buildStructureFromEntryIterator(value.entries(), structureBuilder, structureSchema)
          }

        case ByteGraphValueType.Row =>
          byteGraphRowBuilder.appendRow { structureBuilder =>
            buildForSchemaRowFromEntries(value.asList().asScala.toSeq, structureBuilder, fieldSchema.byteGraphDataType.asInstanceOf[ByteGraphRowType])
          }
      }
    }
  }

  private def buildForSchemaListFromListOfAny(list: java.util.List[com.jsoniter.any.Any],
                                              byteGraphDataType: ByteGraphDataType,
                                              byteGraphListBuilder: ByteGraphListBuilder): Unit = {

    val elements = byteGraphDataType.asInstanceOf[ByteGraphListType].elements
    list.asScala.foreach { any =>
      elements.valueType match {
        case ByteGraphValueType.Int => byteGraphListBuilder.appendInt(any.toInt)
        case ByteGraphValueType.Long => byteGraphListBuilder.appendLong(any.toLong)
        case ByteGraphValueType.Double => byteGraphListBuilder.appendDouble(any.toDouble)
        case ByteGraphValueType.Symbol => byteGraphListBuilder.appendString(any.as(classOf[String]))
        case ByteGraphValueType.String => byteGraphListBuilder.appendString(any.as(classOf[String]))
        case ByteGraphValueType.Date => byteGraphListBuilder.appendDate(LocalDate.parse(any.as(classOf[String])))
        case ByteGraphValueType.DateTime =>
          byteGraphListBuilder.appendDateTime(ZonedDateTime.parse(any.as(classOf[String])))

        case ByteGraphValueType.Duration =>
          byteGraphListBuilder.appendDuration(Duration.parse(any.as(classOf[String])))

        case ByteGraphValueType.Blob =>
          byteGraphListBuilder.appendBlob(Base64.getDecoder.decode(any.as(classOf[String])))

        case ByteGraphValueType.List =>
          byteGraphListBuilder.appendList { listBuilder =>
            buildForSchemaListFromListOfAny(any.asList(), elements, listBuilder)
          }

        case ByteGraphValueType.Structure =>
          val structureSchema: Option[ByteGraphStructureType] = {
            if (!elements.isInstanceOf[ByteGraphStructureType]) None
            else Some(elements.asInstanceOf[ByteGraphStructureType])
          }

          byteGraphListBuilder.appendStructure { structureBuilder =>
            buildStructureFromEntryIterator(any.entries(), structureBuilder, structureSchema)
          }

        case ByteGraphValueType.Row =>
          val rowSchema = elements.asInstanceOf[ByteGraphRowType]
          byteGraphListBuilder.appendRow(rowSchema) { rowBuilder =>
            buildForSchemaRowFromEntries(any.asList().asScala.toSeq, rowBuilder, rowSchema)
          }
      }
    }
  }

}
