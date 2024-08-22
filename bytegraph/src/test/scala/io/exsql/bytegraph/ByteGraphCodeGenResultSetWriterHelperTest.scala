package io.exsql.bytegraph

import java.lang.reflect.{InvocationHandler, Method}
import java.sql.{Date, ResultSet, ResultSetMetaData, Timestamp}
import java.time.{LocalDate, ZonedDateTime}
import java.{sql, util}

import ByteGraph.ByteGraphStructure
import io.exsql.bytegraph.metadata.ByteGraphSchema.{ByteGraphListType, ByteGraphSchemaField}
import io.exsql.bytegraph.metadata.ByteGraphSchema
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteGraphCodeGenResultSetWriterHelperTest extends AnyWordSpec with Matchers {

  "A ByteGraphCodeGenResultSetWriterHelper" when {
    "using a writer generated for a resultSet" should {
      "succeed" in {
        val resultSet: ResultSet = createStubResultSet(Array[AnyRef](
          true.asInstanceOf[AnyRef],
          true.asInstanceOf[AnyRef],
          0x01.toByte.asInstanceOf[AnyRef],
          'a'.asInstanceOf[AnyRef],
          2.toShort.asInstanceOf[AnyRef],
          3.asInstanceOf[AnyRef],
          50L.asInstanceOf[AnyRef],
          2.5f.asInstanceOf[AnyRef],
          1.5d.asInstanceOf[AnyRef],
          "string",
          Utf8Utils.encode("abcdefgh"),
          LocalDate.of(2018,1,1),
          ZonedDateTime.parse("2018-01-01T00:00:00Z"),
          Array("string1", "string2"),
          ByteGraph
            .structureBuilder()
            .appendString("key", "key")
            .appendString("value", "value")
            .build()
        ))

        val byteGraphSchema = ByteGraphSchema.rowOf(
          ByteGraphSchemaField("boolean", ByteGraphValueType.Boolean),
          ByteGraphSchemaField("aBoolean", ByteGraphValueType.Boolean),
          ByteGraphSchemaField("aByte", ByteGraphValueType.Short),
          ByteGraphSchemaField("aChar", ByteGraphValueType.Short),
          ByteGraphSchemaField("aShort", ByteGraphValueType.Short),
          ByteGraphSchemaField("anInt", ByteGraphValueType.Int),
          ByteGraphSchemaField("aLong", ByteGraphValueType.Long),
          ByteGraphSchemaField("aFloat", ByteGraphValueType.Float),
          ByteGraphSchemaField("aDouble", ByteGraphValueType.Double),
          ByteGraphSchemaField("string", ByteGraphValueType.String),
          ByteGraphSchemaField("bytes", ByteGraphValueType.Blob),
          ByteGraphSchemaField("localDate", ByteGraphValueType.Date),
          ByteGraphSchemaField("zonedDateTime", ByteGraphValueType.DateTime),
          ByteGraphSchemaField("strings", ByteGraphListType(ByteGraphValueType.String)),
          ByteGraphSchemaField("dynamic", ByteGraphValueType.Structure)
        )

        val writer = ByteGraph.resultSetWriter(byteGraphSchema, resultSet.getMetaData)
        val bytes = writer.write(resultSet, true)

        val reader = ByteGraph.reader(writer.schema)
        val sqlResponse = reader.read(bytes.bytes())
        sqlResponse(0) should be (java.lang.Boolean.TRUE)
        sqlResponse(1) should be (java.lang.Boolean.TRUE)
        sqlResponse(2) should be (java.lang.Short.valueOf(1.toShort))
        sqlResponse(3) should be (java.lang.Short.valueOf('a'.toShort))
        sqlResponse(4) should be (java.lang.Short.valueOf(2.toShort))
        sqlResponse(5) should be (java.lang.Integer.valueOf(3))
        sqlResponse(6) should be (java.lang.Long.valueOf(50L))
        sqlResponse(7) should be (java.lang.Float.valueOf(2.5f))
        sqlResponse(8) should be (java.lang.Double.valueOf(1.5d))
        sqlResponse(9) should be ("string")
        Utf8Utils.decode(sqlResponse(10).asInstanceOf[Array[Byte]]) should be ("abcdefgh")
        sqlResponse(11) should be (LocalDate.of(2018,1,1))
        sqlResponse(12) should be (ZonedDateTime.parse("2018-01-01T00:00:00Z"))
        util.Arrays.deepEquals(sqlResponse(13).asInstanceOf[Array[AnyRef]], Array("string1", "string2")) should be (true)
        val dynamic = sqlResponse(14).asInstanceOf[ByteGraphStructure]
        dynamic.key.as[String]() should be ("key")
        dynamic.value.as[String]() should be ("value")
      }
    }
  }

  private def createStubResultSet(values: Array[AnyRef]): ResultSet = {
    java.lang.reflect.Proxy.newProxyInstance(
      classOf[ResultSet].getClassLoader,
      Array[Class[_]](classOf[ResultSet]),
      new StubResultSet(values)
    ).asInstanceOf[ResultSet]
  }

  private class StubResultSet(val values: Array[AnyRef]) extends InvocationHandler {
    override def invoke(proxy: Any, method: Method, args: Array[AnyRef]): AnyRef = {
      method.getName match {
        case "getMetaData" => getMetaData
        case "getBoolean" => getBoolean(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getByte" => getByte(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getShort" => getShort(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getInt" => getInt(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getLong" => getLong(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getFloat" => getFloat(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getDouble" => getDouble(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getString" => getString(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getDate" => getDate(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getTimestamp" => getTimestamp(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getBytes" => getBytes(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getArray" => getArray(args(0).asInstanceOf[Integer].intValue()).asInstanceOf[AnyRef]
        case "getObject" => getObject(args(0).asInstanceOf[Integer].intValue())
        case "wasNull" => wasNull.asInstanceOf[AnyRef]
      }
    }

    def getMetaData: ResultSetMetaData = {
      new ResultSetMetaData {
        override def getColumnCount: Int = values.length
        override def getColumnType(index: Int): Int = {
          index match {
            case 1 => java.sql.Types.BOOLEAN
            case 2 => java.sql.Types.BOOLEAN
            case 3 => java.sql.Types.TINYINT
            case 4 => java.sql.Types.CHAR
            case 5 => java.sql.Types.SMALLINT
            case 6 => java.sql.Types.INTEGER
            case 7 => java.sql.Types.BIGINT
            case 8 => java.sql.Types.FLOAT
            case 9 => java.sql.Types.DOUBLE
            case 10 => java.sql.Types.VARCHAR
            case 11 => java.sql.Types.VARBINARY
            case 12 => java.sql.Types.DATE
            case 13 => java.sql.Types.TIMESTAMP
            case 14 => java.sql.Types.ARRAY
            case 15 => java.sql.Types.JAVA_OBJECT
          }
        }

        override def getColumnLabel(index: Int): String = {
          index match {
            case 1 => "boolean"
            case 2 => "aBoolean"
            case 3 => "aByte"
            case 4 => "aChar"
            case 5 => "aShort"
            case 6 => "anInt"
            case 7 => "aLong"
            case 8 => "aFloat"
            case 9 => "aDouble"
            case 10 => "string"
            case 11 => "bytes"
            case 12 => "localDate"
            case 13 => "zonedDateTime"
            case 14 => "strings"
            case 15 => "dynamic"
          }
        }

        override def isAutoIncrement(i: Int): Boolean = ???
        override def isCaseSensitive(i: Int): Boolean = ???
        override def isSearchable(i: Int): Boolean = ???
        override def isCurrency(i: Int): Boolean = ???
        override def isNullable(i: Int): Int = ???
        override def isSigned(i: Int): Boolean = ???
        override def getColumnDisplaySize(i: Int): Int = ???
        override def getColumnName(i: Int): String = ???
        override def getSchemaName(i: Int): String = ???
        override def getPrecision(i: Int): Int = ???
        override def getScale(i: Int): Int = ???
        override def getTableName(i: Int): String = ???
        override def getCatalogName(i: Int): String = ???
        override def getColumnTypeName(i: Int): String = ???
        override def isReadOnly(i: Int): Boolean = ???
        override def isWritable(i: Int): Boolean = ???
        override def isDefinitelyWritable(i: Int): Boolean = ???
        override def getColumnClassName(i: Int): String = ???
        override def unwrap[T](aClass: Class[T]): T = ???
        override def isWrapperFor(aClass: Class[_]): Boolean = ???
      }
    }

    def getBoolean(index: Int): Boolean = values(index - 1).asInstanceOf[Boolean]
    def getByte(index: Int): Byte = values(index - 1).asInstanceOf[Byte]
    def getInt(index: Int): Int = values(index - 1).asInstanceOf[Int]
    def getLong(index: Int): Long = values(index - 1).asInstanceOf[Long]
    def getFloat(index: Int): Float = values(index - 1).asInstanceOf[Float]
    def getDouble(index: Int): Double = values(index - 1).asInstanceOf[Double]
    def getString(index: Int): String = values(index - 1).asInstanceOf[String]
    def getDate(index: Int): Date = Date.valueOf(values(index - 1).asInstanceOf[LocalDate])
    def getTimestamp(index: Int): Timestamp = Timestamp.from(values(index - 1).asInstanceOf[ZonedDateTime].toInstant)
    def getBytes(index: Int): Array[Byte] = values(index - 1).asInstanceOf[Array[Byte]]
    def getArray(index: Int): java.sql.Array = {
      new sql.Array {
        override def getArray: AnyRef = values(index - 1).asInstanceOf[Array[String]]
        override def getBaseTypeName: String = ???
        override def getBaseType: Int = ???
        override def getArray(map: util.Map[String, Class[_]]): AnyRef = ???
        override def getArray(l: Long, i: Int): AnyRef = ???
        override def getArray(l: Long, i: Int, map: util.Map[String, Class[_]]): AnyRef = ???
        override def getResultSet: ResultSet = ???
        override def getResultSet(map: util.Map[String, Class[_]]): ResultSet = ???
        override def getResultSet(l: Long, i: Int): ResultSet = ???
        override def getResultSet(l: Long, i: Int, map: util.Map[String, Class[_]]): ResultSet = ???
        override def free(): Unit = ()
      }
    }

    def getShort(index: Int): Short = {
      index match {
        case 4 => values(3).asInstanceOf[Char].toShort
        case 5 => values(4).asInstanceOf[Short]
      }
    }

    def getObject(index: Int): AnyRef = {
      index match {
        case 15 => values(14)
      }
    }

    def wasNull: Boolean = false

  }

}
