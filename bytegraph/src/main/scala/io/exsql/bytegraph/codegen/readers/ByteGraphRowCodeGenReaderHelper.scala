package io.exsql.bytegraph.codegen.readers

import java.util
import java.util.Locale

import com.google.googlejavaformat.java.Formatter
import io.exsql.bytegraph.codegen.ByteGraphCodeGenClassLoaderHelper.Mod
import io.exsql.bytegraph.codegen.ByteGraphCodeGenReaderHelper._
import io.exsql.bytegraph.codegen._
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphListType, ByteGraphRowType, ByteGraphSchema, ByteGraphSchemaField, ByteGraphStructureType}
import io.exsql.bytegraph.{ByteGraphReader, ByteGraphValueType}
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable

private[codegen] object ByteGraphRowCodeGenReaderHelper {

  private val logger: Logger = LoggerFactory.getLogger("io.exsql.bytegraph.codegen.readers.ByteGraphRowCodeGenReaderHelper")

  def getOrCreate(mod: Mod,
                  readSchema: ByteGraphRowType,
                  schema: ByteGraphRowType,
                  useSqlTypes: Boolean,
                  hasVersionMarkerFlag: Boolean): ByteGraphReader = {

    val sqlSuffix: String = {
      if (useSqlTypes) "Sql"
      else ""
    }

    val parserClassName = s"ByteGraphRowCodeGenReader${sqlSuffix}_${readSchema.signature().toHexString}_${schema.signature().toHexString}"

    val instance: ByteGraphReader = synchronized {
      val classLoader = ByteGraphCodeGenClassLoaderHelper(mod).getClassLoader
      classLoader.tryLoadClass(s"${ByteGraphCodeGenClassLoaderHelper.PackageName}.$parserClassName") match {
        case Some(existing) => existing.getConstructor().newInstance().asInstanceOf[ByteGraphReader]
        case None =>
          val initialPosition: Int = {
            if (hasVersionMarkerFlag) 4
            else 0
          }

          val fields = extractReadColumnPositions(readSchema, schema)
          val variableNameScope = VariableNameScope()
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
               |    int ${variableNameScope.position} = $initialPosition;
               |    byte ${variableNameScope.flag} = ${readByteExpression(variableNameScope.position)};
               |    // positioning after flag
               |    ${variableNameScope.position} += 1;
               |    int ${variableNameScope.flagLength} = ${variableNameScope.flag} & 0x0F;
               |    if (${variableNameScope.flagLength} == 15) {
               |      // skipping actual length value
               |      ${variableNameScope.position} += 4;
               |    }
               |    ${writeRowLoaderFor(fields, readSchema, variableNameScope, useSqlTypes)}
               |    return ${variableNameScope.container};
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

    instance.schema = readSchema
    instance
  }

  private def writeRowLoaderFor(fields: mutable.LinkedHashMap[Int, ByteGraphSchemaField],
                                schema: ByteGraphRowType,
                                variableNameScope: VariableNameScope,
                                useSqlTypes: Boolean): String = {

    s"""
       |final Object[] ${variableNameScope.container} = new Object[${fields.size}];
       |int ${variableNameScope.containerIndex} = 0;
       |
       |// reading offset index size in bytes
       |final int ${variableNameScope.offsetLookupSize} = ${readIntExpression(variableNameScope.position)};
       |${variableNameScope.position} += 4;
       |
       |// beginning of the offset lookup
       |final int ${variableNameScope.offsetLookupOffset} = ${variableNameScope.position};
       |${variableNameScope.position} += ${variableNameScope.offsetLookupSize};
       |
       |// size of the heap
       |final int ${variableNameScope.heapSize} = ${readIntExpression(variableNameScope.position)};
       |${variableNameScope.position} += 4;
       |
       |// beginning of the heap
       |final int ${variableNameScope.heapOffset} = ${variableNameScope.position};
       |
       |int ${variableNameScope.valueOffset} = 0;
       |int ${variableNameScope.valueOffsetLookup} = 0;
       |${fields.map { case (index, field) => writeLoaderFor(field, index, schema, variableNameScope, useSqlTypes) }.mkString("\n")}
     """.stripMargin
  }

  private def writeLoaderFor(field: ByteGraphSchemaField,
                             index: Int,
                             schema: ByteGraphRowType,
                             variableNameScope: VariableNameScope,
                             useSqlTypes: Boolean): String = {

    field.name match {
      case `encodedAssertedGraphFieldName` =>
        s"""
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = super.encodedAssertedGraph();
           |${variableNameScope.containerIndex} += 1;
         """.stripMargin

      case `partitionIdFieldName` =>
        s"""
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = super._partition_id;
           |${variableNameScope.containerIndex} += 1;
         """.stripMargin

      case _ =>
        s"""
           |// finding value offset (tied to int for now)
           |if ($index * 4 < ${variableNameScope.offsetLookupSize}) {
           |  ${variableNameScope.valueOffsetLookup} = ${variableNameScope.offsetLookupOffset} + $index * 4;
           |  ${variableNameScope.valueOffset} = ${readIntExpression(variableNameScope.valueOffsetLookup)};
           |  if (${variableNameScope.valueOffset} != -1) {
           |    ${writeFieldLoader(field, schema, variableNameScope, useSqlTypes)}
           |  }
           |}
           |${variableNameScope.containerIndex} += 1;
        """.stripMargin
    }
  }

  private def writeFieldLoader(field: ByteGraphSchemaField,
                               schema: ByteGraphRowType,
                               variableNameScope: VariableNameScope,
                               useSqlTypes: Boolean): String = {

    (field.byteGraphDataType.valueType: @unchecked) match {
      case ByteGraphValueType.Null => s"${variableNameScope.container}[${variableNameScope.containerIndex}] = null;"
      case ByteGraphValueType.Boolean =>
        s"""
           |if (super.bytes[${variableNameScope.heapOffset} + ${variableNameScope.valueOffset}] == 0x00) ${variableNameScope.container}[${variableNameScope.containerIndex}] = java.lang.Boolean.FALSE;
           |else ${variableNameScope.container}[${variableNameScope.containerIndex}] = java.lang.Boolean.TRUE;
         """.stripMargin

      case ByteGraphValueType.Short | ByteGraphValueType.UShort=>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = Short.valueOf(${readShortExpression(variableNameScope.valuePosition)});
        """.stripMargin

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = Integer.valueOf(${readIntExpression(variableNameScope.valuePosition)});
        """.stripMargin

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = Long.valueOf(${readLongExpression(variableNameScope.valuePosition)});
        """.stripMargin

      case ByteGraphValueType.Float =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = Float.valueOf(${readFloatExpression(variableNameScope.valuePosition)});
        """.stripMargin

      case ByteGraphValueType.Double =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = Double.valueOf(${readDoubleExpression(variableNameScope.valuePosition)});
        """.stripMargin

      case ByteGraphValueType.Date if useSqlTypes =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = java.sql.Date.valueOf(${readDateExpression(variableNameScope.valuePosition)});
         """.stripMargin

      case ByteGraphValueType.Date =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${readDateExpression(variableNameScope.valuePosition)};
         """.stripMargin

      case ByteGraphValueType.DateTime if useSqlTypes =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = java.sql.Timestamp.valueOf(${readDateTimeExpression(variableNameScope.valuePosition)}.toLocalDateTime());
         """.stripMargin

      case ByteGraphValueType.DateTime =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${readDateTimeExpression(variableNameScope.valuePosition)};
         """.stripMargin

      case ByteGraphValueType.Duration if useSqlTypes =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${readDurationExpression(variableNameScope.valuePosition)}.toMillis();
         """.stripMargin

      case ByteGraphValueType.Duration =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${readDurationExpression(variableNameScope.valuePosition)};
         """.stripMargin

      case ByteGraphValueType.String =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |final int ${variableNameScope.valueLength} = ${readIntExpression(variableNameScope.valuePosition)};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${readUtf8Expression(s"${variableNameScope.valuePosition} + 4", variableNameScope.valueLength)};
         """.stripMargin

      case ByteGraphValueType.Blob =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |final int ${variableNameScope.valueLength} = ${readIntExpression(variableNameScope.valuePosition)};
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${readBlobExpression(s"${variableNameScope.valuePosition} + 4", variableNameScope.valueLength)};
         """.stripMargin

      case ByteGraphValueType.List => writeListFieldLoader(
        field,
        field.byteGraphDataType.asInstanceOf[ByteGraphListType].elements.valueType,
        schema,
        variableNameScope,
        useSqlTypes
      )

      case ByteGraphValueType.Row =>
        val nestedSchema = schema.fieldLookup(field.name).byteGraphDataType.asInstanceOf[ByteGraphRowType]
        val nestedFields = extractReadColumnPositions(field.byteGraphDataType.asInstanceOf[ByteGraphRowType], nestedSchema)
        val nestedVariableNameScope = variableNameScope.child()

        s"""
           |int ${nestedVariableNameScope.position} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |// skipping actual length value
           |${nestedVariableNameScope.position} += 4;
           |
           |${writeRowLoaderFor(nestedFields, nestedSchema, nestedVariableNameScope, useSqlTypes)}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${nestedVariableNameScope.container};
         """.stripMargin

      case ByteGraphValueType.Structure if !field.byteGraphDataType.isInstanceOf[ByteGraphStructureType] =>
        s"""
           |final int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
           |final int ${variableNameScope.valueLength} = ${readIntExpression(variableNameScope.valuePosition)};
           |${readStructureExpression(s"${variableNameScope.valuePosition} + 4", variableNameScope.valueLength)}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = structure;
         """.stripMargin

      case ByteGraphValueType.Structure => ???
    }
  }

  private def writeListFieldLoader(field: ByteGraphSchemaField,
                                   elementType: ByteGraphValueType,
                                   rowStructureType: ByteGraphSchema,
                                   variableNameScope: VariableNameScope,
                                   useSqlTypes: Boolean): String = {

    val nestedVariableNameScope = variableNameScope.child()
    val loader = (elementType: @unchecked) match {
      case ByteGraphValueType.Boolean =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Boolean> ${variableNameScope.arrayValues} = new ArrayList<Boolean>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  byte ${nestedVariableNameScope.flag} = ${readByteExpression(nestedVariableNameScope.position)};
           |  int ${nestedVariableNameScope.flagLength} = ${nestedVariableNameScope.flag} & 0x0F;
           |
           |  ${nestedVariableNameScope.arrayPosition} += 1;
           |  ${variableNameScope.arrayValues}.add((${nestedVariableNameScope.flagLength} == 0) ? java.lang.Boolean.FALSE : java.lang.Boolean.TRUE);
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.lang.Boolean[0]);
         """.stripMargin

      case ByteGraphValueType.Short | ByteGraphValueType.UShort =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Short> ${variableNameScope.arrayValues} = new ArrayList<Short>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 3;
           |  ${variableNameScope.arrayValues}.add(Short.valueOf(${readShortExpression(nestedVariableNameScope.position)}));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.lang.Short[0]);
         """.stripMargin

      case ByteGraphValueType.Int | ByteGraphValueType.UInt =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Integer> ${variableNameScope.arrayValues} = new ArrayList<Integer>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 5;
           |  ${variableNameScope.arrayValues}.add(Integer.valueOf(${readIntExpression(nestedVariableNameScope.position)}));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.lang.Integer[0]);
         """.stripMargin

      case ByteGraphValueType.Long | ByteGraphValueType.ULong =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Long> ${variableNameScope.arrayValues} = new ArrayList<Long>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 9;
           |  ${variableNameScope.arrayValues}.add(Long.valueOf(${readLongExpression(nestedVariableNameScope.position)}));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.lang.Long[0]);
         """.stripMargin

      case ByteGraphValueType.Float =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Float> ${variableNameScope.arrayValues} = new ArrayList<Float>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 5;
           |  ${variableNameScope.arrayValues}.add(Float.valueOf(${readFloatExpression(nestedVariableNameScope.position)}));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.lang.Float[0]);
         """.stripMargin

      case ByteGraphValueType.Double =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Double> ${variableNameScope.arrayValues} = new ArrayList<Double>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 9;
           |  ${variableNameScope.arrayValues}.add(Double.valueOf(${readDoubleExpression(nestedVariableNameScope.position)}));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.lang.Double[0]);
         """.stripMargin

      case ByteGraphValueType.Date if useSqlTypes =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<java.sql.Date> ${variableNameScope.arrayValues} = new ArrayList<java.sql.Date>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 4;
           |  final java.sql.Date date = java.sql.Date.valueOf(${readDateExpression(nestedVariableNameScope.position)});
           |  ${variableNameScope.arrayValues}.add(date);
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.sql.Date[0]);
         """.stripMargin

      case ByteGraphValueType.Date =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<java.time.LocalDate> ${variableNameScope.arrayValues} = new ArrayList<java.time.LocalDate>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 4;
           |  final java.time.LocalDate localDate = ${readDateExpression(nestedVariableNameScope.position)};
           |  ${variableNameScope.arrayValues}.add(localDate);
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.time.LocalDate[0]);
         """.stripMargin

      case ByteGraphValueType.DateTime if useSqlTypes =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<java.sql.Timestamp> ${variableNameScope.arrayValues} = new ArrayList<java.sql.Timestamp>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 9;
           |  ${variableNameScope.arrayValues}.add(java.sql.Timestamp.valueOf(${readDateTimeExpression(nestedVariableNameScope.position)}.toLocalDateTime()));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.sql.Timestamp[0]);
         """.stripMargin

      case ByteGraphValueType.DateTime =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<java.time.ZonedDateTime> ${variableNameScope.arrayValues} = new ArrayList<java.time.ZonedDateTime>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 9;
           |  ${variableNameScope.arrayValues}.add(${readDateTimeExpression(nestedVariableNameScope.position)});
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.time.ZonedDateTime[0]);
         """.stripMargin

      case ByteGraphValueType.Duration if useSqlTypes =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Long> ${variableNameScope.arrayValues} = new ArrayList<Long>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 9;
           |  ${variableNameScope.arrayValues}.add(Long.valueOf(${readDurationExpression(nestedVariableNameScope.position)}.toMillis()));
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new Long[0]);
         """.stripMargin

      case ByteGraphValueType.Duration =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<java.time.Duration> ${variableNameScope.arrayValues} = new ArrayList<java.time.Duration>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  ${nestedVariableNameScope.arrayPosition} += 9;
           |  ${variableNameScope.arrayValues}.add(${readDurationExpression(nestedVariableNameScope.position)});
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new java.time.Duration[0]);
         """.stripMargin

      case ByteGraphValueType.String =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<String> ${variableNameScope.arrayValues} = new ArrayList<String>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  byte ${nestedVariableNameScope.flag} = ${readByteExpression(nestedVariableNameScope.position)};
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  int ${nestedVariableNameScope.binaryType} = (${nestedVariableNameScope.flag} & 0xF0) >> Integer.BYTES;
           |  int ${nestedVariableNameScope.flagLength} = ${nestedVariableNameScope.flag} & 0x0F;
           |  if (${nestedVariableNameScope.flagLength} == 15) {
           |    ${nestedVariableNameScope.flagLength} = ${readIntExpression(nestedVariableNameScope.position)};
           |    ${nestedVariableNameScope.position} += 4;
           |  }
           |
           |  ${nestedVariableNameScope.arrayPosition} += ${nestedVariableNameScope.flagLength} + 5;
           |  ${variableNameScope.arrayValues}.add(${readUtf8Expression(nestedVariableNameScope.position, nestedVariableNameScope.flagLength)});
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new String[0]);
         """.stripMargin

      case ByteGraphValueType.Blob =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<byte[]> ${variableNameScope.arrayValues} = new ArrayList<byte[]>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  byte ${nestedVariableNameScope.flag} = ${readByteExpression(nestedVariableNameScope.position)};
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  int ${nestedVariableNameScope.binaryType} = (${nestedVariableNameScope.flag} & 0xF0) >> Integer.BYTES;
           |  int ${nestedVariableNameScope.flagLength} = ${nestedVariableNameScope.flag} & 0x0F;
           |  if (${nestedVariableNameScope.flagLength} == 15) {
           |    ${nestedVariableNameScope.flagLength} = ${readIntExpression(nestedVariableNameScope.position)};
           |    ${nestedVariableNameScope.position} += 4;
           |  }
           |
           |  ${nestedVariableNameScope.arrayPosition} += ${nestedVariableNameScope.flagLength} + 5;
           |  ${variableNameScope.arrayValues}.add(${readBlobExpression(nestedVariableNameScope.position, nestedVariableNameScope.flagLength)});
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new byte[0][]);
         """.stripMargin

      case ByteGraphValueType.Row =>
        val nestedSchema = rowStructureType.fieldLookup(field.name).byteGraphDataType.asInstanceOf[ByteGraphListType].elements.asInstanceOf[ByteGraphRowType]
        val nestedFields = extractReadColumnPositions(field.byteGraphDataType.asInstanceOf[ByteGraphListType].elements.asInstanceOf[ByteGraphRowType], nestedSchema)

        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<Object[]> ${variableNameScope.arrayValues} = new ArrayList<Object[]>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  byte ${nestedVariableNameScope.flag} = ${readByteExpression(nestedVariableNameScope.position)};
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  int ${nestedVariableNameScope.binaryType} = (${nestedVariableNameScope.flag} & 0xF0) >> Integer.BYTES;
           |  int ${nestedVariableNameScope.flagLength} = ${nestedVariableNameScope.flag} & 0x0F;
           |  if (${nestedVariableNameScope.flagLength} == 15) {
           |    ${nestedVariableNameScope.flagLength} = ${readIntExpression(nestedVariableNameScope.position)};
           |    ${nestedVariableNameScope.position} += 4;
           |  }
           |
           |  ${nestedVariableNameScope.arrayPosition} += ${nestedVariableNameScope.flagLength} + 5;
           |  ${writeRowLoaderFor(nestedFields, nestedSchema, nestedVariableNameScope, useSqlTypes)}
           |  ${variableNameScope.arrayValues}.add(${nestedVariableNameScope.container});
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray();
         """.stripMargin

      case ByteGraphValueType.List =>
        s"""
           |int ${variableNameScope.arrayMaxPosition} = ${variableNameScope.valuePosition} + ${variableNameScope.valueLength};
           |final ArrayList<io.exsql.bytegraph.ByteGraph.ByteGraphList> ${variableNameScope.arrayValues} = new ArrayList<io.exsql.bytegraph.ByteGraph.ByteGraphList>();
           |int ${nestedVariableNameScope.arrayPosition} = ${variableNameScope.valuePosition};
           |
           |while (${nestedVariableNameScope.arrayPosition} < ${variableNameScope.arrayMaxPosition}) {
           |  int ${nestedVariableNameScope.position} = ${nestedVariableNameScope.arrayPosition};
           |  //skipping cation type marker
           |  byte ${nestedVariableNameScope.flag} = ${readByteExpression(nestedVariableNameScope.position)};
           |  // positioning after flag
           |  ${nestedVariableNameScope.position} += 1;
           |  int ${nestedVariableNameScope.binaryType} = (${nestedVariableNameScope.flag} & 0xF0) >> Integer.BYTES;
           |  int ${nestedVariableNameScope.flagLength} = ${nestedVariableNameScope.flag} & 0x0F;
           |  if (${nestedVariableNameScope.flagLength} == 15) {
           |    ${nestedVariableNameScope.flagLength} = ${readIntExpression(nestedVariableNameScope.position)};
           |    ${nestedVariableNameScope.position} += 4;
           |  }
           |
           |  ${variableNameScope.arrayValues}.add(new io.exsql.bytegraph.ByteGraph.ByteGraphList(new io.exsql.bytegraph.ByteGraph.ByteGraphValue(io.exsql.bytegraph.bytes.ByteArrayByteGraphBytes.apply(super.bytes, ${nestedVariableNameScope.arrayPosition}, ${nestedVariableNameScope.flagLength} + 5), scala.Option.apply(null)), scala.Option.apply(null)));
           |  ${nestedVariableNameScope.arrayPosition} += ${nestedVariableNameScope.flagLength} + 5;
           |}
           |${variableNameScope.container}[${variableNameScope.containerIndex}] = ${variableNameScope.arrayValues}.toArray(new io.exsql.bytegraph.ByteGraph.ByteGraphList[0]);
         """.stripMargin

      case ByteGraphValueType.Reference => ???
      case ByteGraphValueType.Structure => ???
    }

    s"""
       |int ${variableNameScope.valuePosition} = ${variableNameScope.heapOffset} + ${variableNameScope.valueOffset};
       |int ${variableNameScope.valueLength} = ${readIntExpression(variableNameScope.valuePosition)};
       |${variableNameScope.valuePosition} += 4;
       |if (${variableNameScope.valueLength} > 0) {
       |  $loader
       |}
     """.stripMargin
  }

  private case class VariableNameScope(scope: String = "$") {
    lazy val flag: String = s"${scope}flag".toLowerCase(Locale.ENGLISH)
    lazy val flagLength: String = s"${scope}flagLength".toLowerCase(Locale.ENGLISH)
    lazy val position: String = s"${scope}position".toLowerCase(Locale.ENGLISH)
    lazy val container: String = s"${scope}container".toLowerCase(Locale.ENGLISH)
    lazy val containerIndex: String = s"${scope}containerIndex".toLowerCase(Locale.ENGLISH)
    lazy val offsetLookupSize: String = s"${scope}offsetLookupSize".toLowerCase(Locale.ENGLISH)
    lazy val offsetLookupOffset: String = s"${scope}offsetLookupOffset".toLowerCase(Locale.ENGLISH)
    lazy val heapSize: String = s"${scope}heapSize".toLowerCase(Locale.ENGLISH)
    lazy val heapOffset: String = s"${scope}heapOffset".toLowerCase(Locale.ENGLISH)
    lazy val binaryType: String = s"${scope}binaryType".toLowerCase(Locale.ENGLISH)
    lazy val valueOffset: String = s"${scope}valueOffset".toLowerCase(Locale.ENGLISH)
    lazy val valueOffsetLookup: String = s"${scope}valueOffsetLookup".toLowerCase(Locale.ENGLISH)
    lazy val valuePosition: String = s"${scope}valuePosition".toLowerCase(Locale.ENGLISH)
    lazy val valueLength: String = s"${scope}valueLength".toLowerCase(Locale.ENGLISH)
    lazy val arrayPosition: String = s"${scope}arrayPosition".toLowerCase(Locale.ENGLISH)
    lazy val arrayMaxPosition: String = s"${scope}arrayMaxPosition".toLowerCase(Locale.ENGLISH)
    lazy val arrayValues: String = s"${scope}arrayValues".toLowerCase(Locale.ENGLISH)

    def child(): VariableNameScope = VariableNameScope(scope = s"$$${this.scope}")
  }

}
