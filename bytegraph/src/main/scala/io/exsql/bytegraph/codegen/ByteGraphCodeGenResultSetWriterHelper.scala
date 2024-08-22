package io.exsql.bytegraph.codegen

import java.sql.{ResultSet, ResultSetMetaData, SQLException, Types}
import java.time._

import com.google.googlejavaformat.java.Formatter
import ByteGraphCodeGenClassLoaderHelper.Mod
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphBasicDataType, ByteGraphSchema, ByteGraphSchemaField, ByteGraphStructureType}
import io.exsql.bytegraph.{ByteGraphResultSetWriter, ByteGraphValueType}
import io.exsql.bytegraph.bytes.ByteGraphBytes
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.slf4j.LoggerFactory

object ByteGraphCodeGenResultSetWriterHelper {

  private val logger = LoggerFactory.getLogger("io.exsql.bytegraph.codegen.ByteGraphCodeGenResultSetWriterHelper")

  def getOrCreate(mod: Mod,
                  schema: ByteGraphSchema,
                  resultSetMetaData: ResultSetMetaData): ByteGraphResultSetWriter = {

    val runtimeSchema = resultSetSchema(schema, resultSetMetaData)
    val writerClassName = s"ByteGraphCodeGenResultSetWriter_${schema.signature().toHexString}_${resultSetMetaData.hashCode().toHexString}"

    val instance: ByteGraphResultSetWriter = synchronized {
      val classLoader = ByteGraphCodeGenClassLoaderHelper(mod).getClassLoader
      classLoader.tryLoadClass(s"${ByteGraphCodeGenClassLoaderHelper.PackageName}.$writerClassName") match {
        case Some(existing) => existing.newInstance().asInstanceOf[ByteGraphResultSetWriter]
        case None =>
          val columnRange = 1 to resultSetMetaData.getColumnCount
          val writerClassBody =
            s"""
               |package ${ByteGraphCodeGenClassLoaderHelper.PackageName};
               |import ${classOf[ByteGraphResultSetWriter].getCanonicalName};
               |import ${classOf[LocalDate].getCanonicalName};
               |import ${classOf[LocalDateTime].getCanonicalName};
               |import ${classOf[ZonedDateTime].getCanonicalName};
               |import ${classOf[ZoneOffset].getCanonicalName};
               |import ${classOf[Instant].getCanonicalName};
               |import ${classOf[Duration].getCanonicalName};
               |import ${classOf[ResultSet].getCanonicalName};
               |import ${classOf[SQLException].getCanonicalName};
               |import ${classOf[ByteGraphBytes].getCanonicalName};
               |import ${classOf[ByteGraphValueType].getCanonicalName};
               |public final class $writerClassName extends ${classOf[ByteGraphResultSetWriter].getSimpleName} {
               |  public $writerClassName() {}
               |
               |  @Override
               |  public ByteGraphBytes write(final ResultSet resultSet,
               |                              final boolean writeTypeFlag) throws ClassNotFoundException, SQLException {
               |
               |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes offsets = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
               |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes heap = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
               |
               |    ${columnRange.map(index => generateFieldWriterFor(index, runtimeSchema.fieldLookup(resultSetMetaData.getColumnLabel(index)), resultSetMetaData.getColumnType(index))).mkString("\n")}
               |
               |    final int rowByteCount = offsets.length() + heap.length() + java.lang.Integer.BYTES * 2;
               |
               |    final ByteGraphBytes byteGraphOutput = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply(rowByteCount + 1);
               |    if (writeTypeFlag) byteGraphOutput.writeByte((ByteGraphValueType.Row.typeFlag << 4) + ByteGraphValueType.Row.typeLength);
               |    byteGraphOutput.writeInt(rowByteCount);
               |    byteGraphOutput.writeInt(offsets.length());
               |    byteGraphOutput.write(offsets.bytes());
               |    byteGraphOutput.writeInt(heap.length());
               |    byteGraphOutput.write(heap.bytes());
               |
               |    return byteGraphOutput;
               |  }
               |
               |}
            """.stripMargin

          if (logger.isTraceEnabled) {
            try logger.trace(new Formatter().formatSource(writerClassBody))
            catch {
              case _: Throwable =>
                logger.trace(writerClassBody)
            }
          }

          classLoader.compile(writerClassName, writerClassBody).newInstance().asInstanceOf[ByteGraphResultSetWriter]
      }
    }

    instance.schema = runtimeSchema
    instance
  }

  def generateFieldWriterFor(index: Int, field: ByteGraphSchemaField, sqlType: Int): String = {
    field.byteGraphDataType.valueType match {
      case ByteGraphValueType.Boolean if sqlType == Types.BIT =>
        s"""
           |{
           |  final byte value = resultSet.getByte($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeBoolean((value == 1) ? true : false);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Boolean =>
        s"""
           |{
           |  final boolean value = resultSet.getBoolean($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeBoolean(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Short if sqlType == Types.TINYINT =>
        s"""
           |{
           |  final byte value = resultSet.getByte($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeShort((short) value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Short | ByteGraphValueType.UShort =>
        s"""
           |{
           |  final short value = resultSet.getShort($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeShort(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        s"""
           |{
           |  final int value = resultSet.getInt($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeInt(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        s"""
           |{
           |  final long value = resultSet.getLong($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeLong(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Float =>
        s"""
           |{
           |  final float value = resultSet.getFloat($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeFloat(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Double =>
        s"""
           |{
           |  final double value = resultSet.getDouble($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeDouble(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.String | ByteGraphValueType.Symbol =>
        s"""
           |{
           |  final String value = resultSet.getString($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |
           |    final int lengthOffset = heap.position();
           |    heap.writeInt(0);
           |
           |    final int length = io.exsql.bytegraph.Utf8Utils.encode(value, heap);
           |    heap.writeInt(lengthOffset, length);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Blob =>
        s"""
           |{
           |  final byte[] value = resultSet.getBytes($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    heap.writeInt(value.length);
           |    heap.write(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Date if sqlType == Types.INTEGER =>
        s"""
           |{
           |  final int value = resultSet.getInt($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.localDateToBytes(LocalDateTime.ofInstant(Instant.ofEpochSecond((long) value), ZoneOffset.UTC).toLocalDate(), heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Date if sqlType == Types.BIGINT =>
        s"""
           |{
           |  final long value = resultSet.getLong($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.localDateToBytes(LocalDateTime.ofInstant(Instant.ofEpochSecond((long) value), ZoneOffset.UTC).toLocalDate(), heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Date =>
        s"""
           |{
           |  final LocalDate value = resultSet.getDate($index).toLocalDate();
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.localDateToBytes(value, heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.DateTime if sqlType == Types.INTEGER =>
        s"""
           |{
           |  final int value = resultSet.getInt($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.zonedDateTimeToBytes(ZonedDateTime.ofInstant(Instant.ofEpochSecond((long) value), ZoneOffset.UTC), heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.DateTime if sqlType == Types.BIGINT =>
        s"""
           |{
           |  final long value = resultSet.getLong($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.zonedDateTimeToBytes(ZonedDateTime.ofInstant(Instant.ofEpochSecond(value), ZoneOffset.UTC), heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.DateTime =>
        s"""
           |{
           |  final ZonedDateTime value = resultSet.getTimestamp($index).toInstant().atZone(ZoneOffset.UTC);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.zonedDateTimeToBytes(value, heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Duration if sqlType == Types.BIGINT =>
        s"""
           |{
           |  final long value = resultSet.getLong($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.durationToBytes(Duration.ofMillis(value), heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Duration =>
        s"""
           |{
           |  final int value = resultSet.getInt($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    io.exsql.bytegraph.TemporalValueHelper.durationToBytes(Duration.ofSeconds((long) value), heap);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.List =>
        s"""
           |{
           |  final java.sql.Array value = resultSet.getArray($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |
           |    final int listSizeOffset = heap.position();
           |    heap.writeInt(0);
           |
           |    final Object[] objects = (Object[]) value.getArray();
           |    for (final Object object: objects) {
           |      if (object instanceof String) {
           |        heap.writeByte((ByteGraphValueType.String.typeFlag << 4) + ByteGraphValueType.String.typeLength);
           |        final int lengthOffset = heap.position();
           |        heap.writeInt(0);
           |
           |        final int length = io.exsql.bytegraph.Utf8Utils.encode((String) object, heap);
           |        heap.writeInt(lengthOffset, length);
           |      } else {
           |        throw new IllegalArgumentException("invalid array value: " + object);
           |      }
           |    }
           |
           |    int listTotalSize = heap.position() - (listSizeOffset + java.lang.Integer.BYTES);
           |    heap.writeInt(listSizeOffset, listTotalSize);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Structure if !field.byteGraphDataType.isInstanceOf[ByteGraphStructureType] =>
        s"""
           |{
           |  final Object value = resultSet.getObject($index);
           |  if (resultSet.wasNull()) offsets.writeInt(-1);
           |  else {
           |    offsets.writeInt(heap.position());
           |    final io.exsql.bytegraph.ByteGraph.ByteGraphStructure structure = (io.exsql.bytegraph.ByteGraph.ByteGraphStructure) value;
           |    heap.write(structure.bytes(), 1);
           |  }
           |}
        """.stripMargin
    }
  }

  private def resultSetSchema(schema: ByteGraphSchema, resultSetMetaData: ResultSetMetaData): ByteGraphSchema = {
    var runtimeSchema: ByteGraphSchema = ByteGraphSchema.rowOf()
    (1 to resultSetMetaData.getColumnCount).foreach { index =>
      val sqlType = resultSetMetaData.getColumnType(index)
      val label = resultSetMetaData.getColumnLabel(index)
      runtimeSchema = runtimeSchema.add(
        schema.fieldLookup.getOrElse(label, resultSetSchemaField(label, sqlType))
      )
    }

    runtimeSchema
  }

  private def resultSetSchemaField(label: String, sqlType: Int): ByteGraphSchemaField = {
    val byteGraphValueType: ByteGraphBasicDataType = sqlType match {
      case Types.BOOLEAN | Types.BIT => ByteGraphValueType.Boolean
      case Types.TINYINT | Types.SMALLINT | Types.CHAR => ByteGraphValueType.Short
      case Types.INTEGER => ByteGraphValueType.Int
      case Types.BIGINT => ByteGraphValueType.Long
      case Types.FLOAT => ByteGraphValueType.Float
      case Types.DOUBLE => ByteGraphValueType.Double
      case Types.VARCHAR => ByteGraphValueType.String
      case Types.BLOB | Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY => ByteGraphValueType.Blob
      case Types.DATE => ByteGraphValueType.Date
      case Types.TIMESTAMP => ByteGraphValueType.DateTime
      case Types.ARRAY => ByteGraphValueType.List
    }

    ByteGraphSchemaField(label, byteGraphValueType)
  }

}
