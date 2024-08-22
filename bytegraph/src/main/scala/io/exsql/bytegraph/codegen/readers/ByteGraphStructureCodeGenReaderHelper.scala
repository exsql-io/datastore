package io.exsql.bytegraph.codegen.readers

import java.util
import com.google.googlejavaformat.java.Formatter
import io.exsql.bytegraph.{ByteGraphReader, ByteGraphValueType, HashingUtils, Utf8Utils}
import io.exsql.bytegraph.codegen.ByteGraphCodeGenClassLoaderHelper.Mod
import io.exsql.bytegraph.codegen.ByteGraphCodeGenReaderHelper._
import io.exsql.bytegraph.codegen._
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphDataType, ByteGraphListType, ByteGraphSchemaField, ByteGraphStructureType}
import org.slf4j.LoggerFactory

object ByteGraphStructureCodeGenReaderHelper {

  private val logger = LoggerFactory.getLogger("io.exsql.bytegraph.codegen.readers.ByteGraphStructureCodeGenReaderHelper")

  def getOrCreate(mod: Mod,
                  readSchema: ByteGraphStructureType,
                  schema: ByteGraphStructureType,
                  useSqlTypes: Boolean,
                  hasVersionMarkerFlag: Boolean): ByteGraphReader = {

    val sqlSuffix: String = {
      if (useSqlTypes) "Sql"
      else ""
    }

    val parserClassName = s"ByteGraphStructureCodeGenReader${sqlSuffix}_${readSchema.signature().toHexString}_${schema.signature().toHexString}"

    val instance: ByteGraphReader = synchronized {
      val classLoader = ByteGraphCodeGenClassLoaderHelper(mod).getClassLoader
      classLoader.tryLoadClass(s"${ByteGraphCodeGenClassLoaderHelper.PackageName}.$parserClassName") match {
        case Some(existing) => existing.getConstructor().newInstance().asInstanceOf[ByteGraphReader]
        case None =>
          val fields = readSchema.fields
          val parserClassBody =
            s"""
               |package ${ByteGraphCodeGenClassLoaderHelper.PackageName};
               |import ${classOf[ByteGraphReader].getCanonicalName};
               |import ${classOf[util.ArrayList[_]].getCanonicalName};
               |public final class $parserClassName extends ${classOf[ByteGraphReader].getSimpleName} {
               |  public $parserClassName() {}
               |
               |  @Override
               |  protected Object[] read() throws ClassNotFoundException {
               |    //skipping cation type marker
               |    int position = 4;
               |    byte flag = ${readByteExpression("position")};
               |    // positioning after flag
               |    position += 1;
               |    int flagLength = flag & 0x0F;
               |    if (flagLength == 15) {
               |      // skipping actual length value
               |      position += 4;
               |    }
               |
               |    ${writeStructureLoader(fields, useSqlTypes)}
               |
               |    return container;
               |  }
               |}
             """.stripMargin

          if (logger.isTraceEnabled) {
            try {
              val formatted = new Formatter().formatSource(parserClassBody)
              logger.trace(formatted)
            } catch {
              case _: Throwable => logger.trace(parserClassBody)
            }
          }

          classLoader.compile(parserClassName, parserClassBody).getConstructor().newInstance().asInstanceOf[ByteGraphReader]
      }
    }

    instance
  }

  private def writeStructureLoader(fields: Array[ByteGraphSchemaField],
                                   useSqlTypes: Boolean,
                                   container: String = "container",
                                   position: String = "position",
                                   length: String = "super.bytes.length"): String = {

    val fieldLengthVariableName = s"${container}_fieldLength"
    val fieldNameHashVariableName = s"${container}_fieldNameHash"
    val binaryTypeVariableName = s"${container}_binaryType"
    val valueLengthVariableName = s"${container}_valueLength"

    val (externalFields, internalFields) = fields.zipWithIndex.partition(fieldIndex => isExternalField(fieldIndex._1))
    if (internalFields.isEmpty) {
      s"""
         |final Object[] $container = new Object[${fields.length}];
         |${externalFields.map { case (field, index) => writeExternalLoaderFor(field, index, container) }.mkString("\n") }
      """.stripMargin
    } else {
      s"""
         |final Object[] $container = new Object[${fields.length}];
         |${externalFields.map { case (field, index) => writeExternalLoaderFor(field, index, container) }.mkString("\n") }
         |while ($position < $length) {
         |  flag = ${readByteExpression(position)};
         |  $position += 1;
         |  flagLength = flag & 0x0F;
         |  int $fieldLengthVariableName = flagLength;
         |  if ($fieldLengthVariableName == 15) {
         |    $fieldLengthVariableName = ${readIntExpression(position)};
         |    $position += 4;
         |  }
         |
         |  final int $fieldNameHashVariableName = io.exsql.bytegraph.HashingUtils.hashInt(super.bytes, $position, $fieldLengthVariableName);
         |  $position += $fieldLengthVariableName;
         |  flag = ${readByteExpression(position)};
         |  $position += 1;
         |  flagLength = flag & 0x0F;
         |  final int $binaryTypeVariableName = (flag & 0xF0) >> Integer.BYTES;
         |  switch ($fieldNameHashVariableName) {
         |    ${internalFields.map { case (field, index) => writeCaseFor(field, index, container, position, useSqlTypes) }.mkString("\n") }
         |    default:
         |      if ($binaryTypeVariableName == 2 || $binaryTypeVariableName == 3 || $binaryTypeVariableName == 4 || $binaryTypeVariableName == 5 || $binaryTypeVariableName == 6 || $binaryTypeVariableName == 12) {
         |        if (flagLength > 1) $position += flagLength;
         |      } else {
         |        if ($binaryTypeVariableName == 7 || $binaryTypeVariableName == 8 || $binaryTypeVariableName == 9 || $binaryTypeVariableName == 10 || $binaryTypeVariableName == 11) {
         |          int $valueLengthVariableName = flagLength;
         |          if ($valueLengthVariableName == 15) {
         |            $valueLengthVariableName = ${readIntExpression(position)};
         |            $position += 4;
         |          }
         |
         |          $position += $valueLengthVariableName;
         |        }
         |      }
         |      break;
         |  }
         |}
      """.stripMargin
    }
  }

  private def writeExternalLoaderFor(field: ByteGraphSchemaField, index: Int, container: String): String = {
    field.name match {
      case `encodedAssertedGraphFieldName` =>
        s"""
           |$container[$index] = super.$encodedAssertedGraphFieldName;
         """.stripMargin

      case `partitionIdFieldName` =>
        s"""
           |$container[$index] = super.$partitionIdFieldName;
         """.stripMargin
    }
  }

  private def writeCaseFor(field: ByteGraphSchemaField,
                           index: Int,
                           container: String,
                           position: String,
                           useSqlTypes: Boolean): String = {

    val utf8Bytes = Utf8Utils.encode(field.name)
    val fieldNameHash: Int = HashingUtils.hashInt(utf8Bytes, 0, utf8Bytes.length)
    val binaryTypeVariableName = s"${container}_binaryType"

    s"""
       |case $fieldNameHash:
       |  if ($binaryTypeVariableName != 0) {
       |    ${writeFieldLoader(field, index, container, position, useSqlTypes)}
       |  } else {
       |    $container[$index] = null;
       |  }
       |  break;
    """.stripMargin
  }

  private def writeFieldLoader(field: ByteGraphSchemaField,
                               index: Int,
                               container: String,
                               position: String,
                               useSqlTypes: Boolean): String = {

    val valueLengthVariableName = s"${container}_valueLength"
    val fieldType = field.byteGraphDataType

    (fieldType.valueType: @unchecked) match {
      case ByteGraphValueType.Null => s"$container[$index] = null;"
      case ByteGraphValueType.Boolean =>
        s"""
           |if (flagLength == 0) $container[$index] = java.lang.Boolean.FALSE;
           |else if (flagLength == 1) $container[$index] = java.lang.Boolean.TRUE;
           |else throw new IllegalArgumentException("${field.name} is not a boolean");
        """.stripMargin

      case ByteGraphValueType.Short | ByteGraphValueType.UShort =>
        s"""
           |$container[$index] = java.lang.Short.valueOf(${readShortExpression(position)});
           |$position += 2;
        """.stripMargin

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        s"""
           |$container[$index] = java.lang.Integer.valueOf(${readIntExpression(position)});
           |$position += 4;
        """.stripMargin

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        s"""
           |$container[$index] = java.lang.Long.valueOf(${readLongExpression(position)});
           |$position += 8;
        """.stripMargin

      case ByteGraphValueType.Float =>
        s"""
           |$container[$index] = java.lang.Float.valueOf(${readFloatExpression(position)});
           |$position += 4;
        """.stripMargin

      case ByteGraphValueType.Double =>
        s"""
           |$container[$index] = java.lang.Double.valueOf(${readDoubleExpression(position)});
           |$position += 8;
        """.stripMargin

      case ByteGraphValueType.Date =>
        if (useSqlTypes)
          s"""
             |$container[$index] = java.sql.Date.valueOf(${readDateExpression(position)});
             |$position += 3;
          """.stripMargin else {
          s"""
             |$container[$index] = ${readDateExpression(position)};
             |$position += 3;
          """.stripMargin
        }

      case ByteGraphValueType.DateTime =>
        if (useSqlTypes)
          s"""
             |$container[$index] = java.sql.Timestamp.valueOf(${readDateTimeExpression(position)}.toLocalDateTime());
             |$position += 8;
          """.stripMargin else {
          s"""
             |$container[$index] = ${readDateTimeExpression(position)};
             |$position += 8;
          """.stripMargin
        }

      case ByteGraphValueType.Duration =>
        if (useSqlTypes) {
          s"""
             |$container[$index] = ${readDurationExpression(position)}.toMillis();
             |$position += 8;
          """.stripMargin
        } else {
          s"""
             |$container[$index] = ${readDurationExpression(position)};
             |$position += 8;
          """.stripMargin
        }

      case ByteGraphValueType.String | ByteGraphValueType.Symbol =>
        s"""
           |int $valueLengthVariableName = flagLength;
           |if ($valueLengthVariableName == 15) {
           |  $valueLengthVariableName = ${readIntExpression(position)};
           |  $position += 4;
           |}
           |
           |$container[$index] = ${readUtf8Expression(position, valueLengthVariableName)};
           |$position += $valueLengthVariableName;
        """.stripMargin

      case ByteGraphValueType.Blob =>
        s"""
           |int $valueLengthVariableName = flagLength;
           |if ($valueLengthVariableName == 15) {
           |  $valueLengthVariableName = ${readIntExpression(position)};
           |  $position += 4;
           |}
           |
           |$container[$index] = ${readBlobExpression(position, valueLengthVariableName)};
           |$position += $valueLengthVariableName;
        """.stripMargin

      case ByteGraphValueType.List =>
        writeArrayFieldLoader(field.byteGraphDataType.asInstanceOf[ByteGraphListType].elements, index, container, position, useSqlTypes)

      case ByteGraphValueType.Structure =>
        val nestedFields = field.byteGraphDataType.asInstanceOf[ByteGraphStructureType].fields
        val nestedContainer = s"nested_$container"
        val positionVariableName = s"position_$container"
        val lengthVariableName = s"length_$container"

        s"""
           |int $valueLengthVariableName = flagLength;
           |if ($valueLengthVariableName == 15) {
           |  $valueLengthVariableName = ${readIntExpression(position)};
           |  $position += 4;
           |}
           |
           |final int $lengthVariableName = $position + $valueLengthVariableName;
           |int $positionVariableName = $position;
           |${writeStructureLoader(nestedFields, useSqlTypes, nestedContainer, positionVariableName, lengthVariableName)}
           |$container[$index] = $nestedContainer;
           |$position += $valueLengthVariableName;
        """.stripMargin

      case ByteGraphValueType.Row => ???
      case ByteGraphValueType.Reference => ???
    }
  }

  private def writeArrayFieldLoader(elements: ByteGraphDataType,
                                    index: Int,
                                    container: String,
                                    position: String,
                                    useSqlTypes: Boolean): String = {

    val valueLengthVariableName = s"${container}_valueLength"
    (elements.valueType: @unchecked) match {
      case ByteGraphValueType.Null => s"$container[$index] = null;"
      case ByteGraphValueType.Boolean =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Boolean> list = new ArrayList<Boolean>();
           |  while (arrayPosition < arrayEnd) {
           |    final byte typeFlag = ${readByteExpression("arrayPosition")};
           |    final int typeLength = typeFlag & 0x0F;
           |    arrayPosition += 1;
           |    list.add((typeLength == 0) ? java.lang.Boolean.FALSE : java.lang.Boolean.TRUE);
           |  }
           |  $container[$index] = list.toArray(new Boolean[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Short | ByteGraphValueType.UShort =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Short> list = new ArrayList<Short>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(Short.valueOf(${readShortExpression("arrayPosition")}));
           |    arrayPosition += 2;
           |  }
           |  $container[$index] = list.toArray(new Short[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Integer> list = new ArrayList<Integer>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(Integer.valueOf(${readIntExpression("arrayPosition")}));
           |    arrayPosition += 4;
           |  }
           |  $container[$index] = list.toArray(new Integer[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Long> list = new ArrayList<Long>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(Long.valueOf(${readLongExpression("arrayPosition")}));
           |    arrayPosition += 8;
           |  }
           |  $container[$index] = list.toArray(new Long[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Float =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Float> list = new ArrayList<Float>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(Float.valueOf(${readFloatExpression("arrayPosition")}));
           |    arrayPosition += 4;
           |  }
           |  $container[$index] = list.toArray(new Float[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Double =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Double> list = new ArrayList<Double>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(Double.valueOf(${readDoubleExpression("arrayPosition")}));
           |    arrayPosition += 8;
           |  }
           |  $container[$index] = list.toArray(new Double[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Date if useSqlTypes =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<java.sql.Date> list = new ArrayList<java.sql.Date>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(java.sql.Date.valueOf(${readDateExpression("arrayPosition")}));
           |    arrayPosition += 3;
           |  }
           |  $container[$index] = list.toArray(new java.sql.Date[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Date =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<java.time.LocalDate> list = new ArrayList<java.time.LocalDate>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(${readDateExpression("arrayPosition")});
           |    arrayPosition += 3;
           |  }
           |  $container[$index] = list.toArray(new java.time.LocalDate[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.DateTime if useSqlTypes =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<java.sql.Timestamp> list = new ArrayList<java.sql.Timestamp>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(java.sql.Timestamp.valueOf(${readDateTimeExpression("arrayPosition")}.toLocalDateTime()));
           |    arrayPosition += 8;
           |  }
           |  $container[$index] = list.toArray(new java.sql.Timestamp[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.DateTime =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<java.time.ZonedDateTime> list = new ArrayList<java.time.ZonedDateTime>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(${readDateTimeExpression("arrayPosition")});
           |    arrayPosition += 8;
           |  }
           |  $container[$index] = list.toArray(new java.time.ZonedDateTime[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Duration if useSqlTypes =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<Long> list = new ArrayList<Long>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(${readDurationExpression("arrayPosition")}.toMillis());
           |    arrayPosition += 8;
           |  }
           |  $container[$index] = list.toArray(new Long[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Duration =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<java.time.Duration> list = new ArrayList<java.time.Duration>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    list.add(${readDurationExpression("arrayPosition")});
           |    arrayPosition += 8;
           |  }
           |  $container[$index] = list.toArray(new java.time.Duration[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.String | ByteGraphValueType.Symbol =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<String> list = new ArrayList<String>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    final int stringLength = ${readIntExpression("arrayPosition")};
           |    arrayPosition += 4;
           |    list.add(${readUtf8Expression("arrayPosition", "stringLength")});
           |    arrayPosition += stringLength;
           |  }
           |  $container[$index] = list.toArray(new String[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Blob =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<byte[]> list = new ArrayList<byte[]>();
           |  while (arrayPosition < arrayEnd) {
           |    arrayPosition += 1;
           |    final int blobLength = ${readIntExpression("arrayPosition")};
           |    arrayPosition += 4;
           |    list.add(${readBlobExpression("arrayPosition", "blobLength")});
           |    arrayPosition += blobLength;
           |  }
           |  $container[$index] = list.toArray(new byte[0][]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Structure =>
        val nestedFields = elements.asInstanceOf[ByteGraphStructureType].fields
        val nestedContainer = s"nested_$container"
        val positionVariableName = s"position_$container"
        val lengthVariableName = s"length_$container"
        val entryLengthVariableName = s"entry_length_$container"
        val arrayLimitVariableName = s"array_limit_$container"
        val valuesVariableName = s"values_$container"

        s"""
           |int $valueLengthVariableName = flagLength;
           |if ($valueLengthVariableName == 15) {
           |  $valueLengthVariableName = ${readIntExpression(position)};
           |  $position += 4;
           |}
           |
           |final int $arrayLimitVariableName = $position + $valueLengthVariableName;
           |final ArrayList<Object[]> $valuesVariableName = new ArrayList<Object[]>();
           |int $positionVariableName = $position;
           |while ($positionVariableName < $arrayLimitVariableName) {
           |  flag = ${readByteExpression(positionVariableName)};
           |  $positionVariableName += 1;
           |  flagLength = flag & 0x0F;
           |  int $entryLengthVariableName = flagLength;
           |  if ($entryLengthVariableName == 15) {
           |    $entryLengthVariableName = ${readIntExpression(positionVariableName)};
           |    $positionVariableName += 4;
           |  }
           |
           |  final int $lengthVariableName = $positionVariableName + $entryLengthVariableName;
           |  ${writeStructureLoader(nestedFields, useSqlTypes, nestedContainer, positionVariableName, lengthVariableName)}
           |  $valuesVariableName.add($nestedContainer);
           |}
           |
           |$container[$index] = $valuesVariableName.toArray();
           |$position += $valueLengthVariableName;
        """.stripMargin

      case ByteGraphValueType.List =>
        s"""
           |{
           |  int $valueLengthVariableName = flagLength;
           |  if ($valueLengthVariableName == 15) {
           |    $valueLengthVariableName = ${readIntExpression(position)};
           |    $position += 4;
           |  }
           |
           |  final int arrayPosition = $position;
           |  final int arrayEnd = arrayPosition + $valueLengthVariableName;
           |  final ArrayList<io.exsql.bytegraph.ByteGraph.ByteGraphList> list = new ArrayList<io.exsql.bytegraph.ByteGraph.ByteGraphList>();
           |  while (arrayPosition < arrayEnd) {
           |    final int nestedArrayLength = ${readIntExpression("arrayPosition + 1")};
           |    list.add(new io.exsql.bytegraph.ByteGraph.ByteGraphList(new io.exsql.bytegraph.ByteGraph.ByteGraphValue(io.exsql.bytegraph.bytes.ByteArrayByteGraphBytes.apply(super.bytes, arrayPosition, nestedArrayLength + 5), scala.Option.apply(null)), scala.Option.apply(null)));
           |    arrayPosition += nestedArrayLength + 5;
           |  }
           |  $container[$index] = list.toArray(new io.exsql.bytegraph.ByteGraph.ByteGraphList[0]);
           |  $position += $valueLengthVariableName;
           |}
        """.stripMargin

      case ByteGraphValueType.Row => ???
      case ByteGraphValueType.Reference => ???
    }
  }

}
