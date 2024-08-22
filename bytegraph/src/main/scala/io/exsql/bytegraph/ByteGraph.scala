package io.exsql.bytegraph

import java.sql.ResultSetMetaData
import io.exsql.bytegraph.codegen.ByteGraphCodeGenClassLoaderHelper.Mod
import codegen.{ByteGraphCodeGenClassLoaderHelper, ByteGraphCodeGenReaderHelper, ByteGraphCodeGenResultSetWriterHelper, ByteGraphCodeGenWriterHelper}
import io.exsql.bytegraph.builder.{ByteGraphListBuilder, ByteGraphRowBuilder, ByteGraphStructureBuilder}
import io.exsql.bytegraph.bytes.{ByteArrayByteGraphBytes, ByteGraphBytes, InMemoryByteGraphBytes}
import io.exsql.bytegraph.iterators.{ByteGraphListIterator, ByteGraphRowIterator, ByteGraphStructureIterator}
import io.exsql.bytegraph.lookups.{ByteGraphListLookup, ByteGraphRowLookup, ByteGraphStructureLookup}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphDataType, ByteGraphListType, ByteGraphRowType, ByteGraphSchema, ByteGraphStructureType}
import org.slf4j.LoggerFactory
import scala.language.dynamics

object ByteGraph {

  private val logger = LoggerFactory.getLogger("io.exsql.bytegraph.ByteGraph")

  val VersionFlag: Array[Byte] = Array(0xE0, 0x02, 0x00, 0xEA).map(_.toByte)

  private val byteGraphNull: ByteGraph = {
    ByteGraph.valueOf(ByteArrayByteGraphBytes(
      Array[Byte](((ByteGraphValueType.Null.typeFlag << 4) + ByteGraphValueType.Null.typeLength).toByte)
    ))
  }

  def hasVersionFlag(bytes: ByteGraphBytes): Boolean = {
    bytes.getByte(0) == VersionFlag(0) &&
      bytes.getByte(1) == VersionFlag(1) &&
      bytes.getByte(2) >= VersionFlag(2) &&
      bytes.getByte(3) == VersionFlag(3)
  }

  def valueOf(byteGraphBytes: ByteGraphBytes, schema: Option[ByteGraphSchema] = None): ByteGraphValue = {
    if (hasVersionFlag(byteGraphBytes)) new ByteGraphValue(byteGraphBytes.slice(VersionFlag.length), schema)
    else new ByteGraphValue(byteGraphBytes, schema)
  }

  trait ByteGraph extends Dynamic {
    val bytes: ByteGraphBytes
    val schema: Option[ByteGraphDataType]

    def `type`(): ByteGraphValueType = ByteGraphValueType.readValueTypeFromByte(bytes.getByte(0))

    def as[T](): T = fromValueType[T](this.`type`(), schema)

    def option[T](): Option[T] = {
      val valueType = this.`type`()
      valueType match {
        case ByteGraphValueType.Null => None
        case _ => Some(fromValueType[T](valueType, schema))
      }
    }

    def selectDynamic(field: String): ByteGraph = {
      `type`() match {
        case ByteGraphValueType.Structure =>
          fromValueType[ByteGraphStructure](this.`type`(), schema)(field).getOrElse(byteGraphNull)

        case ByteGraphValueType.Row =>
          fromValueType[ByteGraphRow](this.`type`(), schema)(field).getOrElse(byteGraphNull)

        case _ =>
          logger.warn(s"$field accessor is not defined on a ${this.`type`()}")
          byteGraphNull
      }
    }

    private def fromValueType[T](valueType: ByteGraphValueType, schema: Option[ByteGraphDataType]): T = {
      val value: Any = {
        (valueType: @unchecked) match {
          case ByteGraphValueType.Null => null
          case ByteGraphValueType.Boolean => (bytes.getByte(0) & 0x0F) == 0x01
          case ByteGraphValueType.Short => bytes.getShort(1)
          case ByteGraphValueType.Int => bytes.getInt(1)
          case ByteGraphValueType.Long => bytes.getLong(1)
          case ByteGraphValueType.UShort => bytes.getShort(1)
          case ByteGraphValueType.UInt => bytes.getInt(1)
          case ByteGraphValueType.ULong => bytes.getLong(1)
          case ByteGraphValueType.Float => bytes.getFloat(1)
          case ByteGraphValueType.Double => bytes.getDouble(1)
          case ByteGraphValueType.Date => TemporalValueHelper.toLocalDate(bytes.getBytes(1))
          case ByteGraphValueType.DateTime => TemporalValueHelper.toDateTime(bytes.getBytes(1))
          case ByteGraphValueType.Duration => TemporalValueHelper.toDuration(bytes.getBytes(1))
          case ByteGraphValueType.Reference => ???
          case ByteGraphValueType.Symbol => ???
          case ByteGraphValueType.String =>
            val length = bytes.getInt(1)
            Utf8Utils.decode(bytes.getBytes(5, length))

          case ByteGraphValueType.Blob =>
            val length = bytes.getInt(1)
            bytes.getBytes(5, length)

          case ByteGraphValueType.List => ByteGraphList(valueOf(bytes), schema.map(_.asInstanceOf[ByteGraphListType].elements))
          case ByteGraphValueType.Structure => ByteGraphStructure(valueOf(bytes), schema.map(_.asInstanceOf[ByteGraphStructureType]))
          case ByteGraphValueType.Row => ByteGraphRow(valueOf(bytes), schema.get.asInstanceOf[ByteGraphRowType])
        }
      }

      value.asInstanceOf[T]
    }

  }

  class ByteGraphValue private[bytegraph] (override val bytes: ByteGraphBytes,
                                           override val schema: Option[ByteGraphSchema]) extends ByteGraph

  trait ByteGraphValueContainer extends ByteGraph {
    def underlying: ByteGraphValue
    override val bytes: ByteGraphBytes = underlying.bytes
  }

  case class ByteGraphList(override val underlying: ByteGraphValue,
                           elementType: Option[ByteGraphDataType] = None) extends ByteGraphValueContainer {

    lazy private val lookup = new ByteGraphListLookup(this)

    override def `type`(): ByteGraphValueType = ByteGraphValueType.List
    override val schema: Option[ByteGraphDataType] = elementType.map(ByteGraphListType)
    def iterator: Iterator[ByteGraph] = ByteGraph.listIterator(this, elementType)
    def apply(index: Int): Option[ByteGraph] = lookup(index)
  }

  case class ByteGraphStructure(override val underlying: ByteGraphValue,
                                override val schema: Option[ByteGraphSchema] = None) extends ByteGraphValueContainer {

    lazy private val lookup = new ByteGraphStructureLookup(this)

    override def `type`(): ByteGraphValueType = ByteGraphValueType.Structure
    def iterator: Iterator[(String, ByteGraph)] = ByteGraph.structureIterator(this, schema)
    def apply(index: Int): Option[ByteGraph] = lookup(index)
    def apply(field: String): Option[ByteGraph] = {
      if (schema.exists(_.fieldIndexLookup.contains(field))) schema.get.fieldIndexLookup.get(field).flatMap(apply)
      else lookup(field)
    }

    override def selectDynamic(field: String): ByteGraph = {
      apply(field).getOrElse(byteGraphNull)
    }

  }

  case class ByteGraphRow(override val underlying: ByteGraphValue,
                          rowSchema: ByteGraphSchema) extends ByteGraphValueContainer {

    lazy private val lookup = new ByteGraphRowLookup(this)

    override def `type`(): ByteGraphValueType = ByteGraphValueType.Row
    override val schema: Option[ByteGraphSchema] = Some(rowSchema)

    def iterator: Iterator[ByteGraph] = ByteGraph.rowIterator(this, rowSchema)
    def apply(index: Int): Option[ByteGraph] = lookup(index)
    def apply(field: String): Option[ByteGraph] = {
      rowSchema.fieldIndexLookup.get(field).flatMap(apply)
    }

    override def selectDynamic(field: String): ByteGraph = {
      rowSchema.fieldIndexLookup.get(field) match {
        case None =>
          logger.warn(s"$field is not defined on a the schema")
          byteGraphNull

        case Some(value) => apply(value).getOrElse(byteGraphNull)
      }
    }

  }

  def render(value: ByteGraph, withVersionFlag: Boolean = false): InMemoryByteGraphBytes = {
    render(value.bytes, withVersionFlag)
  }

  def render(value: ByteGraphBytes, withVersionFlag: Boolean): InMemoryByteGraphBytes = {
    val expectedBytes = value.length() + { if (withVersionFlag) 1 else 0 }

    val byteGraphOutput = InMemoryByteGraphBytes(expectedBytes)
    if (withVersionFlag) byteGraphOutput.write(VersionFlag)

    byteGraphOutput.write(value)
    byteGraphOutput.close()
    byteGraphOutput
  }

  def partialReader(readSchema: ByteGraphSchema,
                    schema: ByteGraphSchema,
                    mod: Mod = ByteGraphCodeGenClassLoaderHelper.Shared,
                    useSqlTypes: Boolean = false): ByteGraphReader = {

    ByteGraphCodeGenReaderHelper.getOrCreate(
      mod = mod,
      readSchema = readSchema,
      schema = schema,
      useSqlTypes = useSqlTypes
    )
  }

  def reader(byteGraphSchema: ByteGraphSchema,
             mod: Mod = ByteGraphCodeGenClassLoaderHelper.Shared,
             useSqlTypes: Boolean = false): ByteGraphReader = {

    ByteGraphCodeGenReaderHelper.getOrCreate(
      mod = mod,
      readSchema = byteGraphSchema,
      schema = byteGraphSchema,
      useSqlTypes = useSqlTypes
    )
  }

  def writer(byteGraphSchema: ByteGraphSchema,
             mod: Mod = ByteGraphCodeGenClassLoaderHelper.Shared): ByteGraphWriter = {

    ByteGraphCodeGenWriterHelper.getOrCreate(mod, byteGraphSchema)
  }

  def resultSetWriter(byteGraphSchema: ByteGraphSchema,
                      resultSetMetaData: ResultSetMetaData,
                      mod: Mod = ByteGraphCodeGenClassLoaderHelper.Shared): ByteGraphResultSetWriter = {

    ByteGraphCodeGenResultSetWriterHelper.getOrCreate(
      mod = mod,
      schema = byteGraphSchema,
      resultSetMetaData = resultSetMetaData
    )
  }

  def listBuilder(): ByteGraphListBuilder = {
    val inMemoryByteGraphOutput = InMemoryByteGraphBytes()
    inMemoryByteGraphOutput.writeByte((ByteGraphValueType.List.typeFlag << 4) + ByteGraphValueType.List.typeLength)
    inMemoryByteGraphOutput.writeInt(0)

    new ByteGraphListBuilder(inMemoryByteGraphOutput)
  }

  def listBuilderFrom(byteGraphList: ByteGraphList): ByteGraphListBuilder = {
    val buffer = ByteGraph.render(byteGraphList)
    new ByteGraphListBuilder(buffer)
  }

  def structureBuilder(): ByteGraphStructureBuilder = {
    val inMemoryByteGraphOutput = InMemoryByteGraphBytes()
    inMemoryByteGraphOutput.writeByte((ByteGraphValueType.Structure.typeFlag << 4) + ByteGraphValueType.Structure.typeLength)
    inMemoryByteGraphOutput.writeInt(0)

    new ByteGraphStructureBuilder(inMemoryByteGraphOutput)
  }

  def structureBuilderFrom(byteGraphList: ByteGraphList): ByteGraphStructureBuilder = {
    val buffer = ByteGraph.render(byteGraphList)
    new ByteGraphStructureBuilder(buffer)
  }

  def rowBuilder(schema: ByteGraphRowType): ByteGraphRowBuilder = {
    val offsetsLength = schema.fields.length * java.lang.Integer.BYTES
    val offsets = InMemoryByteGraphBytes(offsetsLength + 3 * java.lang.Integer.BYTES + 1)
    offsets.writeByte((ByteGraphValueType.Row.typeFlag << 4) + ByteGraphValueType.Row.typeLength)
    offsets.writeInt(0)
    offsets.writeInt(offsetsLength)

    new ByteGraphRowBuilder(offsets, InMemoryByteGraphBytes(), schema)
  }

  def toJsonString(byteGraph: ByteGraph, renderAsDocument: Boolean = false): String = {
    Utf8Utils.decode(toJson(byteGraph, renderAsDocument))
  }

  def toJson(byteGraph: ByteGraph, renderAsDocument: Boolean = false): Array[Byte] = {
    ByteGraphJsonHelper.toJson(byteGraph, renderAsDocument)
  }

  def fromJson(json: String, schema: Option[ByteGraphSchema] = None): ByteGraph = {
    ByteGraphJsonHelper.fromJson(json, schema)
  }

  private def listIterator(byteGraphList: ByteGraphList, elementType: Option[ByteGraphDataType]): Iterator[ByteGraph] = {
    new ByteGraphListIterator(byteGraphList, elementType)
  }

  private def rowIterator(byteGraphRow: ByteGraphRow, schema: ByteGraphSchema): Iterator[ByteGraph] = {
    new ByteGraphRowIterator(byteGraphRow, schema)
  }

  private def structureIterator(byteGraphStructure: ByteGraphStructure,
                                schema: Option[ByteGraphSchema]): Iterator[(String, ByteGraph)] = {

    new ByteGraphStructureIterator(byteGraphStructure, schema)
  }

}
