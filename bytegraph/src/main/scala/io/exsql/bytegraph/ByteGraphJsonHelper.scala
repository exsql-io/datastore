package io.exsql.bytegraph

import java.time.format.DateTimeFormatter
import java.time.{Duration, LocalDate, ZonedDateTime}
import java.util.Base64

import com.jsoniter.output.JsonStream
import com.jsoniter.{JsonIterator, ValueType}
import ByteGraph.{ByteGraph, ByteGraphList, ByteGraphRow, ByteGraphStructure}
import io.exsql.bytegraph.builder.{ByteGraphBuilderHelper, ByteGraphListBuilder, ByteGraphStructureBuilder}
import io.exsql.bytegraph.bytes.{ByteGraphBytes, InMemoryByteGraphBytes}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphDataType, ByteGraphListType, ByteGraphRowType, ByteGraphSchema, ByteGraphSchemaField, ByteGraphStructureType}

import scala.collection.JavaConverters._
import scala.util.Failure

private[bytegraph] object ByteGraphJsonHelper {

  def fromJson(json: String, schema: Option[ByteGraphSchema] = None): ByteGraph = {
    val value = JsonIterator.deserialize(json)
    schema match {
      case None => fromAny(value, None)
      case Some(schema) if schema.isInstanceOf[ByteGraphRowType] => ByteGraphJsonRowHelper.fromJsonRow(value, schema.asInstanceOf[ByteGraphRowType])
      case Some(schema) => fromAny(value, Some(schema.asInstanceOf[ByteGraphStructureType]))
    }
  }

  def toJson(byteGraphBytes: ByteGraphBytes, renderAsDocument: Boolean): Array[Byte] = {
    toJson(ByteGraph.valueOf(byteGraphBytes), renderAsDocument)
  }

  def toJson(byteGraph: ByteGraph, renderAsDocument: Boolean): Array[Byte] = {
    val outputStream = new FasterByteArrayOutputStream()
    val jsonStream = new JsonStream(outputStream, 1024)
    appendByteGraph(byteGraph, jsonStream, renderAsDocument)
    jsonStream.close()
    outputStream.bytes()
  }

  private def fromAny(any: com.jsoniter.any.Any, schema: Option[ByteGraphStructureType]): ByteGraph = {
    val byteGraphBytes = InMemoryByteGraphBytes()
    any.valueType() match {
      case ValueType.NULL =>
        ByteGraphBuilderHelper.writeNull(byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ValueType.ARRAY => fromListOfAny(any.asList(), schema)
      case ValueType.OBJECT => fromEntryIterator(any.entries(), schema)
      case ValueType.BOOLEAN =>
        ByteGraphBuilderHelper.writeBoolean(any.toBoolean, byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ValueType.NUMBER =>
        val bigDecimal: BigDecimal = any.toBigDecimal
        if (bigDecimal.isValidInt) ByteGraphBuilderHelper.writeInt(bigDecimal.toIntExact, byteGraphBytes)
        else if (bigDecimal.isValidLong) ByteGraphBuilderHelper.writeLong(bigDecimal.toLongExact, byteGraphBytes)
        else if (bigDecimal.isExactDouble) ByteGraphBuilderHelper.writeDouble(bigDecimal.toDouble, byteGraphBytes)
        else throw new IllegalArgumentException(s"can't convert NUMBER: $bigDecimal")

        ByteGraph.valueOf(byteGraphBytes)

      case ValueType.STRING =>
        ByteGraphBuilderHelper.writeString(any.as(classOf[String]), byteGraphBytes)
        ByteGraph.valueOf(byteGraphBytes)

      case ValueType.INVALID => throw new IllegalArgumentException(s"unsupported type: ${ValueType.INVALID}")
    }
  }

  private def fromListOfAny(list: java.util.List[com.jsoniter.any.Any], schema: Option[ByteGraphStructureType]): ByteGraph = {
    val byteGraphListBuilder = ByteGraph.listBuilder()
    buildListFromListOfAny(list, byteGraphListBuilder, schema)

    byteGraphListBuilder.build()
  }

  private[bytegraph] def buildListFromListOfAny(list: java.util.List[com.jsoniter.any.Any],
                                                byteGraphListBuilder: ByteGraphListBuilder,
                                                schema: Option[ByteGraphStructureType]): Unit = {

    list.asScala.foreach { any =>
      any.valueType() match {
        case ValueType.NULL => byteGraphListBuilder.appendNull()
        case ValueType.ARRAY => byteGraphListBuilder.appendList { nestedListBuilder =>
          buildListFromListOfAny(any.asList(), nestedListBuilder, schema)
        }

        case ValueType.OBJECT => byteGraphListBuilder.appendStructure { nestedStructureBuilder =>
          buildStructureFromEntryIterator(any.entries(), nestedStructureBuilder, schema)
        }

        case ValueType.BOOLEAN => byteGraphListBuilder.appendBoolean(any.toBoolean)
        case ValueType.NUMBER =>
          val bigDecimal: BigDecimal = any.toBigDecimal
          if (bigDecimal.isValidInt) byteGraphListBuilder.appendInt(bigDecimal.toIntExact)
          else if (bigDecimal.isValidLong) byteGraphListBuilder.appendLong(bigDecimal.toLongExact)
          else if (bigDecimal.isExactDouble) byteGraphListBuilder.appendDouble(bigDecimal.toDouble)
          else Failure(new IllegalArgumentException(s"can't convert NUMBER: $bigDecimal"))

        case ValueType.STRING => byteGraphListBuilder.appendString(any.toString)
        case ValueType.INVALID => throw new IllegalArgumentException(s"unsupported type: ${ValueType.INVALID}")
      }
    }
  }

  private def fromEntryIterator(entries: com.jsoniter.any.Any.EntryIterator,
                                schema: Option[ByteGraphStructureType]): ByteGraph = {

    val byteGraphStructureBuilder = ByteGraph.structureBuilder()
    buildStructureFromEntryIterator(entries, byteGraphStructureBuilder, schema)
    byteGraphStructureBuilder.build()
  }

  private[bytegraph] def buildStructureFromEntryIterator(entries: com.jsoniter.any.Any.EntryIterator,
                                                         byteGraphStructureBuilder: ByteGraphStructureBuilder,
                                                         schema: Option[ByteGraphStructureType]): Unit = {

    schema match {
      case None => inferStructureFromEntryIterator(entries, byteGraphStructureBuilder)
      case Some(jsonSchema) =>
        buildForSchemaStructureFromEntryIterator(entries, byteGraphStructureBuilder, jsonSchema)
    }
  }

  private def buildForSchemaStructureFromEntryIterator(entries: com.jsoniter.any.Any.EntryIterator,
                                                       byteGraphStructureBuilder: ByteGraphStructureBuilder,
                                                       schema: ByteGraphStructureType): Unit = {

    while (entries.next()) {
      val field = entries.key()
      val any = entries.value()

      schema.fieldLookup.get(field) match {
        case None => inferTypeFromField(field, any, byteGraphStructureBuilder)
        case Some(fieldSchema) =>
          fieldTypeFromFieldWithSchema(field, any, fieldSchema, byteGraphStructureBuilder, schema)
      }
    }
  }

  private def buildForSchemaListFromListOfAny(list: java.util.List[com.jsoniter.any.Any],
                                              byteGraphDataType: ByteGraphDataType,
                                              byteGraphListBuilder: ByteGraphListBuilder,
                                              schema: ByteGraphStructureType): Unit = {

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
            buildForSchemaListFromListOfAny(any.asList(), elements, listBuilder, schema)
          }

        case ByteGraphValueType.Structure =>
          val structureSchema: Option[ByteGraphStructureType] = {
            if (!byteGraphDataType.isInstanceOf[ByteGraphStructureType]) None
            else Some(byteGraphDataType.asInstanceOf[ByteGraphStructureType])
          }

          byteGraphListBuilder.appendStructure { structureBuilder =>
            buildStructureFromEntryIterator(any.entries(), structureBuilder, structureSchema)
          }
      }
    }
  }

  private def fieldTypeFromFieldWithSchema(field: String,
                                           value: com.jsoniter.any.Any,
                                           fieldSchema: ByteGraphSchemaField,
                                           byteGraphStructureBuilder: ByteGraphStructureBuilder,
                                           schema: ByteGraphStructureType): Unit = {

    fieldSchema.byteGraphDataType.valueType match {
      case ByteGraphValueType.Null => byteGraphStructureBuilder.appendNull(field)
      case ByteGraphValueType.Boolean => byteGraphStructureBuilder.appendBoolean(field, value.toBoolean)
      case ByteGraphValueType.Short => byteGraphStructureBuilder.appendShort(field, value.toInt.toShort)
      case ByteGraphValueType.UShort => byteGraphStructureBuilder.appendUShort(field, value.toInt.toShort)
      case ByteGraphValueType.Int => byteGraphStructureBuilder.appendInt(field, value.toInt)
      case ByteGraphValueType.UInt => byteGraphStructureBuilder.appendUInt(field, value.toInt)
      case ByteGraphValueType.Long => byteGraphStructureBuilder.appendLong(field, value.toLong)
      case ByteGraphValueType.ULong => byteGraphStructureBuilder.appendULong(field, value.toLong)
      case ByteGraphValueType.Float => byteGraphStructureBuilder.appendFloat(field, value.toFloat)
      case ByteGraphValueType.Double => byteGraphStructureBuilder.appendDouble(field, value.toDouble)
      case ByteGraphValueType.Symbol => byteGraphStructureBuilder.appendString(field, value.as(classOf[String]))
      case ByteGraphValueType.String => byteGraphStructureBuilder.appendString(field, value.as(classOf[String]))
      case ByteGraphValueType.Date =>
        byteGraphStructureBuilder.appendDate(field, LocalDate.parse(value.as(classOf[String])))

      case ByteGraphValueType.DateTime =>
        byteGraphStructureBuilder.appendDateTime(field, ZonedDateTime.parse(value.as(classOf[String])))

      case ByteGraphValueType.Duration =>
        byteGraphStructureBuilder.appendDuration(field, Duration.parse(value.as(classOf[String])))

      case ByteGraphValueType.Blob =>
        byteGraphStructureBuilder.appendBlob(field, Base64.getDecoder.decode(value.as(classOf[String])))

      case ByteGraphValueType.List =>
        byteGraphStructureBuilder.appendList(field) { listBuilder =>
          buildForSchemaListFromListOfAny(value.asList(), fieldSchema.byteGraphDataType, listBuilder, schema)
        }

      case ByteGraphValueType.Structure =>
        val structureSchema: Option[ByteGraphStructureType] = {
          if (!fieldSchema.byteGraphDataType.isInstanceOf[ByteGraphStructureType]) None
          else Some(fieldSchema.byteGraphDataType.asInstanceOf[ByteGraphStructureType])
        }

        byteGraphStructureBuilder.appendStructure(field) { structureBuilder =>
          buildStructureFromEntryIterator(value.entries(), structureBuilder, structureSchema)
        }
    }
  }

  private def inferStructureFromEntryIterator(entries: com.jsoniter.any.Any.EntryIterator,
                                              byteGraphStructureBuilder: ByteGraphStructureBuilder): Unit = {

    while (entries.next()) {
      val field = entries.key()
      val any = entries.value()

      inferTypeFromField(field, any, byteGraphStructureBuilder)
    }
  }

  private def inferTypeFromField(field: String,
                                 value: com.jsoniter.any.Any,
                                 byteGraphStructureBuilder: ByteGraphStructureBuilder): Unit = {

    value.valueType() match {
      case ValueType.NULL => byteGraphStructureBuilder.appendNull(field)
      case ValueType.ARRAY => byteGraphStructureBuilder.appendList(field) { nestedListBuilder =>
        buildListFromListOfAny(value.asList(), nestedListBuilder, schema = None)
      }

      case ValueType.OBJECT => byteGraphStructureBuilder.appendStructure(field) { nestedStructureBuilder =>
        buildStructureFromEntryIterator(value.entries(), nestedStructureBuilder, schema = None)
      }

      case ValueType.BOOLEAN => byteGraphStructureBuilder.appendBoolean(field, value.toBoolean)
      case ValueType.NUMBER =>
        val bigDecimal: BigDecimal = value.toBigDecimal
        if (bigDecimal.isValidInt) byteGraphStructureBuilder.appendInt(field, bigDecimal.toIntExact)
        else if (bigDecimal.isValidLong) byteGraphStructureBuilder.appendLong(field, bigDecimal.toLongExact)
        else byteGraphStructureBuilder.appendDouble(field, bigDecimal.toDouble)

      case ValueType.STRING => byteGraphStructureBuilder.appendString(field, AnyToString.toString(value))
      case ValueType.INVALID => throw new IllegalArgumentException(s"unsupported type: ${ValueType.INVALID}")
    }
  }

  private def appendByteGraph(byteGraph: ByteGraph, jsonStream: JsonStream, renderAsDocument: Boolean): Unit = {
    byteGraph.`type`() match {
      case ByteGraphValueType.Null => jsonStream.writeNull()
      case ByteGraphValueType.Boolean => jsonStream.writeVal(byteGraph.as[Boolean]())
      case ByteGraphValueType.Blob => jsonStream.writeVal(Base64.getEncoder.encodeToString(byteGraph.as[Array[Byte]]()))
      case ByteGraphValueType.String => jsonStream.writeVal(byteGraph.as[String]())
      case ByteGraphValueType.Symbol => jsonStream.writeVal(byteGraph.as[String]())
      case ByteGraphValueType.Short | ByteGraphValueType.UShort => jsonStream.writeVal(byteGraph.as[Short]())
      case ByteGraphValueType.Int | ByteGraphValueType.UInt | ByteGraphValueType.Reference => jsonStream.writeVal(byteGraph.as[Int]())
      case ByteGraphValueType.Long | ByteGraphValueType.ULong => jsonStream.writeVal(byteGraph.as[Long]())
      case ByteGraphValueType.Float => jsonStream.writeVal(byteGraph.as[Float]())
      case ByteGraphValueType.Double => jsonStream.writeVal(byteGraph.as[Double]())
      case ByteGraphValueType.Date => jsonStream.writeVal(byteGraph.as[LocalDate]().format(DateTimeFormatter.ISO_DATE))
      case ByteGraphValueType.DateTime => jsonStream.writeVal(byteGraph.as[ZonedDateTime]().format(DateTimeFormatter.ISO_DATE_TIME))
      case ByteGraphValueType.Duration => jsonStream.writeVal(byteGraph.as[Duration]().toString)
      case ByteGraphValueType.List =>
        val byteGraphList = {
          byteGraph match {
            case list: ByteGraphList => list
            case _ => byteGraph.as[ByteGraphList]()
          }
        }

        jsonStream.writeArrayStart()

        val iterator = byteGraphList.iterator
        while (iterator.hasNext) {
          val byteGraphValue = iterator.next()
          appendByteGraph(byteGraphValue, jsonStream, renderAsDocument)
          if (iterator.hasNext) jsonStream.writeMore()
        }

        jsonStream.writeArrayEnd()

      case ByteGraphValueType.Structure =>
        val byteGraphStructure = {
          byteGraph match {
            case structure: ByteGraphStructure => structure
            case _ => byteGraph.as[ByteGraphStructure]()
          }
        }

        jsonStream.writeObjectStart()

        val iterator = byteGraphStructure.iterator
        while (iterator.hasNext) {
          val (name, byteGraphValue) = iterator.next()
          jsonStream.writeObjectField(name)
          appendByteGraph(byteGraphValue, jsonStream, renderAsDocument)
          if (iterator.hasNext) jsonStream.writeMore()
        }

        jsonStream.writeObjectEnd()

      case ByteGraphValueType.Row =>
        val byteGraphRow = {
          byteGraph match {
            case row: ByteGraphRow => row
            case _ => byteGraph.as[ByteGraphRow]()
          }
        }

        if (renderAsDocument) {
          jsonStream.writeObjectStart()

          var index = 0
          val iterator = byteGraphRow.iterator
          while (iterator.hasNext) {
            val field = byteGraphRow.rowSchema.fields(index)
            jsonStream.writeObjectField(field.name)

            val byteGraphValue = iterator.next()
            appendByteGraph(byteGraphValue, jsonStream, renderAsDocument)
            if (iterator.hasNext) jsonStream.writeMore()

            index += 1
          }

          jsonStream.writeObjectEnd()
        } else {
          jsonStream.writeArrayStart()

          val iterator = byteGraphRow.iterator
          while (iterator.hasNext) {
            val byteGraphValue = iterator.next()
            appendByteGraph(byteGraphValue, jsonStream, renderAsDocument)
            if (iterator.hasNext) jsonStream.writeMore()
          }

          jsonStream.writeArrayEnd()
        }
    }
  }

}
