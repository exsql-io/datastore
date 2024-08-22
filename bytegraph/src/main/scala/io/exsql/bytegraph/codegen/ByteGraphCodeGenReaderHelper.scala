package io.exsql.bytegraph.codegen

import ByteGraphCodeGenClassLoaderHelper.Mod
import io.exsql.bytegraph.ByteGraphReader
import io.exsql.bytegraph.codegen.readers.{ByteGraphRowCodeGenReaderHelper, ByteGraphStructureCodeGenReaderHelper}
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphRowType, ByteGraphSchema, ByteGraphStructureType}

object ByteGraphCodeGenReaderHelper {

  def getOrCreate(mod: Mod,
                  readSchema: ByteGraphSchema,
                  schema: ByteGraphSchema,
                  useSqlTypes: Boolean = false,
                  hasVersionMarkerFlag: Boolean = true): ByteGraphReader = {

    schema match {
      case byteGraphRowType: ByteGraphRowType => ByteGraphRowCodeGenReaderHelper.getOrCreate(
        mod, readSchema.asInstanceOf[ByteGraphRowType], byteGraphRowType, useSqlTypes, hasVersionMarkerFlag
      )

      case byteGraphStructureType: ByteGraphStructureType => ByteGraphStructureCodeGenReaderHelper.getOrCreate(
        mod, readSchema.asInstanceOf[ByteGraphStructureType], byteGraphStructureType, useSqlTypes, hasVersionMarkerFlag
      )
    }
  }

  def readByteExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""$byteArrayFieldName[$positionFieldName]"""
  }

  def readShortExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""(short) ($byteArrayFieldName[$positionFieldName] << 8 | ($byteArrayFieldName[$positionFieldName + 1] & 0xFF))"""
  }

  def readIntExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""$byteArrayFieldName[$positionFieldName] << 24 | ($byteArrayFieldName[$positionFieldName + 1] & 0xFF) << 16 | ($byteArrayFieldName[$positionFieldName + 2] & 0xFF) << 8 | ($byteArrayFieldName[$positionFieldName + 3] & 0xFF)"""
  }

  def readLongExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""($byteArrayFieldName[$positionFieldName] & 0xFFL) << 56 | ($byteArrayFieldName[$positionFieldName + 1] & 0xFFL) << 48 | ($byteArrayFieldName[$positionFieldName + 2] & 0xFFL) << 40 | ($byteArrayFieldName[$positionFieldName + 3] & 0xFFL) << 32 | ($byteArrayFieldName[$positionFieldName + 4] & 0xFFL) << 24 | ($byteArrayFieldName[$positionFieldName + 5] & 0xFFL) << 16 | ($byteArrayFieldName[$positionFieldName + 6] & 0xFFL) << 8 | ($byteArrayFieldName[$positionFieldName + 7] & 0xFFL)"""
  }

  def readFloatExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""java.lang.Float.intBitsToFloat(${readIntExpression(positionFieldName, byteArrayFieldName)})"""
  }

  def readDoubleExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""java.lang.Double.longBitsToDouble(${readLongExpression(positionFieldName, byteArrayFieldName)})"""
  }

  def readDateExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""io.exsql.bytegraph.TemporalValueHelper.toLocalDate($byteArrayFieldName, $positionFieldName)"""
  }

  def readDateTimeExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""io.exsql.bytegraph.TemporalValueHelper.toDateTime($byteArrayFieldName, $positionFieldName)"""
  }

  def readDurationExpression(positionFieldName: String, byteArrayFieldName: String = "super.bytes"): String = {
    s"""io.exsql.bytegraph.TemporalValueHelper.toDuration($byteArrayFieldName, $positionFieldName)"""
  }

  def readUtf8Expression(positionFieldName: String,
                         valueLengthFieldName: String,
                         byteArrayFieldName: String = "super.bytes"): String = {

    s"""io.exsql.bytegraph.Utf8Utils.decode($byteArrayFieldName, $positionFieldName, $valueLengthFieldName)"""
  }

  def readBlobExpression(positionFieldName: String,
                         valueLengthFieldName: String,
                         byteArrayFieldName: String = "super.bytes"): String = {

    s"""io.exsql.bytegraph.ByteArrayUtils.copy($byteArrayFieldName, $positionFieldName, $valueLengthFieldName)"""
  }

  def readStructureExpression(positionFieldName: String,
                              valueLengthFieldName: String,
                              byteArrayFieldName: String = "super.bytes"): String = {

    s"""
       |final io.exsql.bytegraph.bytes.InMemoryByteGraphBytes byteGraphBytes = io.exsql.bytegraph.bytes.InMemoryByteGraphBytes.apply();
       |byteGraphBytes.writeByte((io.exsql.bytegraph.ByteGraphValueType.Structure.typeFlag << 4) + io.exsql.bytegraph.ByteGraphValueType.Structure.typeLength);
       |byteGraphBytes.write($byteArrayFieldName, $positionFieldName - Integer.BYTES, $valueLengthFieldName + Integer.BYTES);
       |byteGraphBytes.close();
       |
       |final io.exsql.bytegraph.ByteGraph.ByteGraphStructure structure = new io.exsql.bytegraph.ByteGraph.ByteGraphStructure(io.exsql.bytegraph.ByteGraph.valueOf(byteGraphBytes, scala.Option.apply(null)), scala.Option.apply(null));
     """.stripMargin
  }

}
