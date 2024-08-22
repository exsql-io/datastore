package io.exsql.bytegraph.metadata

import com.jsoniter.{JsonIterator, ValueType}
import io.exsql.bytegraph.ByteGraph.ByteGraphStructure
import io.exsql.bytegraph.{ByteGraph, ByteGraphValueType}
import io.exsql.bytegraph.builder.{ByteGraphListBuilder, ByteGraphStructureBuilder}
import net.openhft.hashing.LongHashFunction

import java.util.Locale
import scala.collection.mutable
import scala.jdk.CollectionConverters._

object ByteGraphSchema {

  def rowOf(fields: ByteGraphSchemaField*): ByteGraphRowType = ByteGraphRowType(fields.toArray)
  def structureOf(fields: ByteGraphSchemaField*): ByteGraphStructureType = ByteGraphStructureType(fields.toArray)
  def listOf(elements: ByteGraphDataType): ByteGraphListType = ByteGraphListType(elements)
  def referenceOf(field: String): ByteGraphReferenceType = ByteGraphReferenceType(field)
  def fromJson(json: String): ByteGraphSchema = {
    fromJsonSchema(JsonIterator.deserialize(json))
  }

  trait ByteGraphDataType extends Any {
    def valueType: ByteGraphValueType
    def signature(): Long = LongHashFunction.xx().hashInts(Array(valueType.typeFlag, valueType.typeLength))
    override def toString: String = valueType.name()
    protected[metadata] def appendDataType(to: ByteGraphStructureBuilder): Unit
  }

  implicit class ByteGraphBasicDataType(override val valueType: ByteGraphValueType) extends AnyVal with ByteGraphDataType {
    override protected[metadata] def appendDataType(to: ByteGraphStructureBuilder): Unit = {
      to.appendString("type", valueType.name().toLowerCase(Locale.US))
    }
  }

  case class ByteGraphSchemaField(name: String,
                                  byteGraphDataType: ByteGraphDataType,
                                  metadata: mutable.Map[String, String] = mutable.Map.empty) {

    def signature(): Long = {
      LongHashFunction.xx().hashLongs(Array(LongHashFunction.xx().hashChars(name), byteGraphDataType.signature()))
    }

    override def toString: String = {
      s"ByteGraphSchemaField($name,$byteGraphDataType,${metadata.mkString("{", ",", "}")})"
    }

    private[metadata] def appendField(to: ByteGraphStructureBuilder): Unit = {
      to.appendString("name", name)
      byteGraphDataType.appendDataType(to)
      to.appendStructure("metadata") { metaDataBuilder =>
        metadata.foreach { case (key, value) =>
          metaDataBuilder.appendString(key, value)
        }
      }
    }
  }

  case class ByteGraphListType private[ByteGraphSchema](elements: ByteGraphDataType) extends ByteGraphDataType {
    override val valueType: ByteGraphValueType = ByteGraphValueType.List
    override def signature(): Long = {
      LongHashFunction.xx().hashLongs(Array(elements.signature(), LongHashFunction.xx().hashInt(valueType.typeFlag)))
    }

    override protected[metadata] def appendDataType(to: ByteGraphStructureBuilder): Unit = {
      to.appendString("type", "list")
      to.appendStructure("elements")(elements.appendDataType)
    }
  }

  case class ByteGraphReferenceType private[ByteGraphSchema](field: String) extends ByteGraphDataType {
    override val valueType: ByteGraphValueType = ByteGraphValueType.Reference
    override def signature(): Long = {
      LongHashFunction.xx().hashLongs(Array(
        LongHashFunction.xx().hashChars(field),
        LongHashFunction.xx().hashInt(valueType.typeFlag)
      ))
    }

    override protected[metadata] def appendDataType(to: ByteGraphStructureBuilder): Unit = {
      to.appendString("type", "reference")
      to.appendString("field", field)
    }
  }

  trait ByteGraphSchema extends ByteGraphDataType {
    val fields: Array[ByteGraphSchemaField]
    def fieldLookup: Map[String, ByteGraphSchemaField]
    lazy val fieldIndexLookup: Map[String, Int] = fields.map(_.name).zipWithIndex.toMap

    lazy val byteGraph: ByteGraphStructure = {
      val builder = ByteGraph.structureBuilder()
      appendDataType(builder)
      builder.build()
    }

    def add(field: ByteGraphSchemaField): ByteGraphSchema
    override def signature(): Long = {
      LongHashFunction.xx().hashLongs(Array(
        LongHashFunction.xx().hashLongs(fields.map(_.signature())),
        LongHashFunction.xx().hashInt(valueType.typeFlag)
      ))
    }

    def json(): String = ByteGraph.toJsonString(byteGraph)

    private[metadata] def appendFields(to: ByteGraphListBuilder): Unit = {
      val iterator = fields.iterator
      while (iterator.hasNext) {
        val field = iterator.next()
        to.appendStructure(field.appendField)
      }
    }

  }

  case class ByteGraphRowType private[ByteGraphSchema](override val fields: Array[ByteGraphSchemaField]) extends ByteGraphSchema {
    override lazy val fieldLookup: Map[String, ByteGraphSchemaField] = fields.map(field => field.name -> field).toMap
    override val valueType: ByteGraphValueType = ByteGraphValueType.Row
    override def add(field: ByteGraphSchemaField): ByteGraphSchema = {
      this.copy(fields = this.fields ++ Array(field))
    }

    override def toString: String = {
      s"ByteGraphRowType(${fields.map(_.toString).mkString(",")})"
    }

    override protected[metadata] def appendDataType(to: ByteGraphStructureBuilder): Unit = {
      to.appendString("type", "row")
      to.appendList("fields")(appendFields)
    }
  }

  case class ByteGraphStructureType private[ByteGraphSchema](override val fields: Array[ByteGraphSchemaField]) extends ByteGraphSchema {
    override lazy val fieldLookup: Map[String, ByteGraphSchemaField] = fields.map(field => field.name -> field).toMap
    override val valueType: ByteGraphValueType = ByteGraphValueType.Structure
    override def add(field: ByteGraphSchemaField): ByteGraphSchema = {
      this.copy(fields = this.fields ++ Array(field))
    }

    override def toString: String = {
      s"ByteGraphStructureType(${fields.map(_.toString).mkString(",")})"
    }

    override protected[metadata] def appendDataType(to: ByteGraphStructureBuilder): Unit = {
      to.appendString("type", "structure")
      to.appendList("fields")(appendFields)
    }
  }

  private def fromJsonSchema(schema: com.jsoniter.any.Any): ByteGraphSchema = {
    val fields = schema.get("fields").asList()
    schema.get("type").as(classOf[String]) match {
      case "row" => ByteGraphRowType(fromJsonFields(fields.asScala.toSeq))
      case "structure" => ByteGraphStructureType(fromJsonFields(fields.asScala.toSeq))
    }
  }

  private def fromJsonFields(seq: Seq[com.jsoniter.any.Any]): Array[ByteGraphSchemaField] = {
    seq.map(fromJsonField).toArray
  }

  private def fromJsonField(field: com.jsoniter.any.Any): ByteGraphSchemaField = {
    ByteGraphSchemaField(
      name = field.get("name").as(classOf[String]),
      byteGraphDataType = fromJsonType(field)
    )
  }

  private def fromJsonType(field: com.jsoniter.any.Any): ByteGraphDataType = {
    val fields = field.get("fields")
    field.get("type").as(classOf[String]) match {
      case "structure" if fields.valueType() == ValueType.INVALID => ByteGraphValueType.Structure
      case "structure" => ByteGraphStructureType(fromJsonFields(fields.asList().asScala.toSeq))
      case "row" => ByteGraphRowType(fromJsonFields(fields.asList().asScala.toSeq))
      case "list" => ByteGraphListType(fromJsonType(field.get("elements")))
      case name => ByteGraphValueType.fromName(name)
    }
  }

}
