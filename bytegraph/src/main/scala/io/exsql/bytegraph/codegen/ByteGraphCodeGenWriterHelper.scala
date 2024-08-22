package io.exsql.bytegraph.codegen

import java.sql.{ResultSet, SQLException}
import java.time._

import com.google.googlejavaformat.java.Formatter
import ByteGraphCodeGenClassLoaderHelper.Mod
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphListType, ByteGraphReferenceType, ByteGraphRowType, ByteGraphSchema, ByteGraphSchemaField, ByteGraphStructureType}
import io.exsql.bytegraph.{ByteGraphValueType, ByteGraphWriter}
import io.exsql.bytegraph.bytes.{ByteGraphBytes, InMemoryByteGraphBytes}
import org.slf4j.LoggerFactory

object ByteGraphCodeGenWriterHelper {

  private val logger = LoggerFactory.getLogger("io.exsql.bytegraph.codegen.ByteGraphCodeGenWriterHelper")

  def getOrCreate(mod: Mod, schema: ByteGraphSchema): ByteGraphWriter = {
    val writerClassName = s"ByteGraphCodeGenWriter_${schema.signature().toHexString}"

    val instance: ByteGraphWriter = synchronized {
      val classLoader = ByteGraphCodeGenClassLoaderHelper(mod).getClassLoader
      classLoader.tryLoadClass(s"${ByteGraphCodeGenClassLoaderHelper.PackageName}.$writerClassName") match {
        case Some(existing) => existing.newInstance().asInstanceOf[ByteGraphWriter]
        case None =>
          val fields = schema.fields.zipWithIndex.map { case (field, index) =>
            field.byteGraphDataType match {
              case byteGraphReferenceType: ByteGraphReferenceType =>
                field -> schema.fieldIndexLookup(byteGraphReferenceType.field)

              case _ => field -> index
            }
          }

          var indexOffset: Int = 0
          val writers: Array[String] = fields.map { case (field, index) =>
            val writer = generateFieldWriterFor(index - indexOffset, field)
            if (field.byteGraphDataType.valueType == ByteGraphValueType.Reference) indexOffset += 1

            writer
          }

          val writerClassBody =
            s"""
               |package ${ByteGraphCodeGenClassLoaderHelper.PackageName};
               |import ${classOf[ByteGraphWriter].getCanonicalName};
               |import ${classOf[LocalDate].getCanonicalName};
               |import ${classOf[LocalDateTime].getCanonicalName};
               |import ${classOf[ZonedDateTime].getCanonicalName};
               |import ${classOf[ZoneOffset].getCanonicalName};
               |import ${classOf[Instant].getCanonicalName};
               |import ${classOf[Duration].getCanonicalName};
               |import ${classOf[ResultSet].getCanonicalName};
               |import ${classOf[SQLException].getCanonicalName};
               |import ${classOf[ByteGraphBytes].getCanonicalName};
               |import ${classOf[InMemoryByteGraphBytes].getCanonicalName};
               |import ${classOf[ByteGraphValueType].getCanonicalName};
               |public final class $writerClassName extends ${classOf[ByteGraphWriter].getSimpleName} {
               |  public $writerClassName() {}
               |
               |  @Override
               |  public ByteGraphBytes write(final Object[] row,
               |                              final boolean writeTypeFlag) throws ClassNotFoundException, SQLException {
               |
               |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes offsets = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
               |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes heap = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
               |
               |    ${writers.mkString("\n")}
               |
               |    final int rowByteCount = offsets.length() + heap.length() + java.lang.Integer.BYTES * 2;
               |
               |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes byteGraphOutput = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply(rowByteCount + 1);
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

          classLoader.compile(writerClassName, writerClassBody).newInstance().asInstanceOf[ByteGraphWriter]
      }
    }

    instance.schema = schema
    instance
  }

  def generateFieldWriterFor(index: Int,
                             field: ByteGraphSchemaField,
                             offsetFieldName: String = "offsets",
                             heapFieldName: String = "heap",
                             rowFieldName: String = "row",
                             listSizeOffsetFieldName: String = "listSizeOffset",
                             objectsFieldName: String = "objects",
                             objectFieldName: String = "object"): String = {

    field.byteGraphDataType.valueType match {
      case ByteGraphValueType.Null => s"$offsetFieldName.writeInt(-1);"
      case ByteGraphValueType.Boolean =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    $heapFieldName.writeBoolean((Boolean) $rowFieldName[$index]);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Short | ByteGraphValueType.UShort =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    $heapFieldName.writeShort((Short) $rowFieldName[$index]);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    $heapFieldName.writeInt((Integer) $rowFieldName[$index]);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    $heapFieldName.writeLong((Long) $rowFieldName[$index]);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Float =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    $heapFieldName.writeFloat((Float) $rowFieldName[$index]);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Double =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    $heapFieldName.writeDouble((Double) $rowFieldName[$index]);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.String | ByteGraphValueType.Symbol =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    final int lengthOffset = $heapFieldName.position();
           |    $heapFieldName.writeInt(0);
           |
           |    final int length = io.exsql.bytegraph.Utf8Utils.encode((String) $rowFieldName[$index], $heapFieldName);
           |    $heapFieldName.writeInt(lengthOffset, length);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Blob =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    final byte[] value = (byte[]) $rowFieldName[$index];
           |    $heapFieldName.writeInt(value.length);
           |    $heapFieldName.write(value);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Date =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    if ($rowFieldName[$index] instanceof LocalDate) io.exsql.bytegraph.TemporalValueHelper.localDateToBytes((LocalDate) $rowFieldName[$index], $heapFieldName);
           |    else io.exsql.bytegraph.TemporalValueHelper.localDateToBytes(((java.sql.Date) $rowFieldName[$index]).toLocalDate(), $heapFieldName);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.DateTime =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    if ($rowFieldName[$index] instanceof ZonedDateTime) io.exsql.bytegraph.TemporalValueHelper.zonedDateTimeToBytes((ZonedDateTime) $rowFieldName[$index], $heapFieldName);
           |    else io.exsql.bytegraph.TemporalValueHelper.zonedDateTimeToBytes(((java.sql.Timestamp) $rowFieldName[$index]).toInstant().atZone(ZoneOffset.UTC), $heapFieldName);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Duration =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    if ($rowFieldName[$index] instanceof Duration) io.exsql.bytegraph.TemporalValueHelper.durationToBytes((Duration) $rowFieldName[$index], $heapFieldName);
           |    else io.exsql.bytegraph.TemporalValueHelper.durationToBytes(Duration.ofMillis((Long) $rowFieldName[$index]), $heapFieldName);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.List =>
        val elements = field.byteGraphDataType.asInstanceOf[ByteGraphListType].elements
        var indexOffset = 0
        val fields: Array[(ByteGraphSchemaField, Int)] = {
          if (elements.valueType == ByteGraphValueType.Row) {
            val nested: ByteGraphRowType = elements.asInstanceOf[ByteGraphRowType]
            nested.fields.zipWithIndex.map { case (field, index) =>
              field.byteGraphDataType match {
                case byteGraphReferenceType: ByteGraphReferenceType =>
                  field -> nested.fieldIndexLookup(byteGraphReferenceType.field)

                case _ => field -> index
              }
            }
          } else Array.empty
        }

        val writers = fields.map { case (field, index) =>
          val writer = generateFieldWriterFor(
            index - indexOffset,
            field,
            s"$$$offsetFieldName",
            s"$$$heapFieldName",
            s"$$$rowFieldName",
            s"$$$listSizeOffsetFieldName",
            s"$$$objectsFieldName",
            s"$$$objectFieldName"
          )

          if (field.byteGraphDataType.valueType == ByteGraphValueType.Reference) indexOffset += 1

          writer
        }

        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    final int $listSizeOffsetFieldName = $heapFieldName.position();
           |    $heapFieldName.writeInt(0);
           |
           |    final Object[] $objectsFieldName = (Object[]) $rowFieldName[$index];
           |    for (final Object $objectFieldName: $objectsFieldName) {
           |      if ($objectFieldName instanceof String) {
           |        $heapFieldName.writeByte((ByteGraphValueType.String.typeFlag << 4) + ByteGraphValueType.String.typeLength);
           |        final int lengthOffset = $heapFieldName.position();
           |        $heapFieldName.writeInt(0);
           |
           |        final int length = io.exsql.bytegraph.Utf8Utils.encode((String) $objectFieldName, $heapFieldName);
           |        $heapFieldName.writeInt(lengthOffset, length);
           |      } else if ($objectFieldName instanceof Object[]) {
           |        if ($objectFieldName == null) $heapFieldName.writeByte((ByteGraphValueType.Null.typeFlag << 4) + ByteGraphValueType.Null.typeLength);
           |        else {
           |          final Object[] $$$rowFieldName = (Object[]) $objectFieldName;
           |
           |          final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes $$$offsetFieldName = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
           |          final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes $$$heapFieldName = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
           |
           |          ${writers.mkString("\n")}
           |
           |          $heapFieldName.writeByte((ByteGraphValueType.Row.typeFlag << 4) + ByteGraphValueType.Row.typeLength);
           |          $heapFieldName.writeInt($$$offsetFieldName.length() + $$$heapFieldName.length() + java.lang.Integer.BYTES * 2);
           |          $heapFieldName.writeInt($$$offsetFieldName.length());
           |          $heapFieldName.write($$$offsetFieldName.bytes());
           |          $heapFieldName.writeInt($$$heapFieldName.length());
           |          $heapFieldName.write($$$heapFieldName.bytes());
           |        }
           |      } else {
           |        throw new IllegalArgumentException("invalid array value: " + $objectFieldName);
           |      }
           |    }
           |
           |    int listTotalSize = $heapFieldName.position() - ($listSizeOffsetFieldName + java.lang.Integer.BYTES);
           |    $heapFieldName.writeInt($listSizeOffsetFieldName, listTotalSize);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Structure if !field.byteGraphDataType.isInstanceOf[ByteGraphStructureType] =>
        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |    final io.exsql.bytegraph.ByteGraph.ByteGraphStructure structure = (io.exsql.bytegraph.ByteGraph.ByteGraphStructure) $rowFieldName[$index];
           |    $heapFieldName.write(structure.bytes(), 1);
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Row =>
        var indexOffset = 0
        val nested = field.byteGraphDataType.asInstanceOf[ByteGraphRowType]
        val fields = nested.fields.zipWithIndex.map { case (field, index) =>
          field.byteGraphDataType match {
            case byteGraphReferenceType: ByteGraphReferenceType =>
              field -> nested.fieldIndexLookup(byteGraphReferenceType.field)

            case _ => field -> index
          }
        }

        val writers = fields.map { case (field, index) =>
          val writer = generateFieldWriterFor(
            index - indexOffset,
            field,
            s"$$$offsetFieldName",
            s"$$$heapFieldName",
            s"$$$rowFieldName",
            s"$$$listSizeOffsetFieldName",
            s"$$$objectsFieldName",
            s"$$$objectFieldName"
          )

          if (field.byteGraphDataType.valueType == ByteGraphValueType.Reference) indexOffset += 1

          writer
        }

        s"""
           |{
           |  if ($index >= $rowFieldName.length || $rowFieldName[$index] == null) $offsetFieldName.writeInt(-1);
           |  else {
           |    $offsetFieldName.writeInt($heapFieldName.position());
           |
           |    final Object[] $$$rowFieldName = (Object[]) $rowFieldName[$index];
           |
           |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes $$$offsetFieldName = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
           |    final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes $$$heapFieldName = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
           |
           |    ${writers.mkString("\n")}
           |
           |    $heapFieldName.writeInt($$$offsetFieldName.length() + $$$heapFieldName.length() + java.lang.Integer.BYTES * 2);
           |    $heapFieldName.writeInt($$$offsetFieldName.length());
           |    $heapFieldName.write($$$offsetFieldName.bytes());
           |    $heapFieldName.writeInt($$$heapFieldName.length());
           |    $heapFieldName.write($$$heapFieldName.bytes());
           |  }
           |}
        """.stripMargin

      case ByteGraphValueType.Reference =>
        s"""
           |$offsetFieldName.writeInt($offsetFieldName.getInt($index * Integer.BYTES));
        """.stripMargin
    }
  }

}
