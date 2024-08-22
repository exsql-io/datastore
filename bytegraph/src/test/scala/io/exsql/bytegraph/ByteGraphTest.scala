package io.exsql.bytegraph

import java.nio.charset.StandardCharsets
import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}

import ByteGraph.{ByteGraphList, ByteGraphRow, ByteGraphStructure}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteGraphTest extends AnyWordSpec with Matchers {

  "A ByteGraph" when {

    "reading using a schema" should {

      "succeed reading a row with all field types" in withRow { byteGraphBytes =>
        val byteGraphReader = ByteGraph.reader(byteGraphRowSchema)
        val row = byteGraphReader.read(byteGraphBytes.bytes.bytes())

        row(0) should be (null)
        row(1) should be (java.lang.Boolean.TRUE)
        row(2) should be (java.lang.Boolean.FALSE)
        row(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        row(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        row(5) should be (java.lang.Long.valueOf(Long.MaxValue))

        java.lang.Short.toUnsignedInt(row(6).asInstanceOf[java.lang.Short]) should be (65535)
        java.lang.Integer.toUnsignedLong(row(7).asInstanceOf[Integer]) should be (4294967295L)
        java.lang.Long.toUnsignedString(row(8).asInstanceOf[java.lang.Long]) should be (
          "18446744073709551615"
        )

        row(9) should be (java.lang.Float.valueOf(Float.MaxValue))
        row(10) should be (java.lang.Double.valueOf(Double.MaxValue))
        row(11) should be (LocalDate.of(2019, 1, 1))
        row(12) should be (ZonedDateTime.of(
          LocalDate.of(2019, 1, 1),
          LocalTime.of(23, 59, 59),
          ZoneOffset.UTC
        ))

        row(13) should be (java.time.Duration.ofSeconds(3600))
        row(14) should be ("string")
        new String(row(15).asInstanceOf[Array[Byte]], StandardCharsets.UTF_8) should be ("blob")

        val nested: Array[AnyRef] = row(16).asInstanceOf[Array[AnyRef]]
        nested(0) should be (null)
        nested(1) should be (java.lang.Boolean.TRUE)
        nested(2) should be (java.lang.Boolean.FALSE)
        nested(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        nested(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        nested(5) should be (java.lang.Long.valueOf(Long.MaxValue))

        val booleans: Array[java.lang.Boolean] = row(17).asInstanceOf[Array[java.lang.Boolean]]
        booleans(0) should be (java.lang.Boolean.TRUE)
        booleans(1) should be (java.lang.Boolean.FALSE)
        booleans(2) should be (java.lang.Boolean.TRUE)

        val integers: Array[java.lang.Integer] = row(18).asInstanceOf[Array[java.lang.Integer]]
        integers(0) should be (Int.MaxValue - 2)
        integers(1) should be (Int.MaxValue - 1)
        integers(2) should be (Int.MaxValue)

        val uIntegers: Array[java.lang.Integer] = row(19).asInstanceOf[Array[java.lang.Integer]]
        java.lang.Integer.toUnsignedLong(uIntegers(0)) should be (4294967293L)
        java.lang.Integer.toUnsignedLong(uIntegers(1)) should be (4294967294L)
        java.lang.Integer.toUnsignedLong(uIntegers(2)) should be (4294967295L)

        val shorts: Array[java.lang.Short] = row(20).asInstanceOf[Array[java.lang.Short]]
        shorts(0) should be (Short.MaxValue - 2)
        shorts(1) should be (Short.MaxValue - 1)
        shorts(2) should be (Short.MaxValue)

        val uShorts: Array[java.lang.Short] = row(21).asInstanceOf[Array[java.lang.Short]]
        java.lang.Short.toUnsignedInt(uShorts(0)) should be (65533)
        java.lang.Short.toUnsignedInt(uShorts(1)) should be (65534)
        java.lang.Short.toUnsignedInt(uShorts(2)) should be (65535)

        val longs: Array[java.lang.Long] = row(22).asInstanceOf[Array[java.lang.Long]]
        longs(0) should be (Long.MaxValue - 2)
        longs(1) should be (Long.MaxValue - 1)
        longs(2) should be (Long.MaxValue)

        val uLongs: Array[java.lang.Long] = row(23).asInstanceOf[Array[java.lang.Long]]
        java.lang.Long.toUnsignedString(uLongs(0)) should be ("18446744073709551613")
        java.lang.Long.toUnsignedString(uLongs(1)) should be ("18446744073709551614")
        java.lang.Long.toUnsignedString(uLongs(2)) should be ("18446744073709551615")

        val floats: Array[java.lang.Float] = row(24).asInstanceOf[Array[java.lang.Float]]
        floats(0) should be (Float.MaxValue - 2)
        floats(1) should be (Float.MaxValue - 1)
        floats(2) should be (Float.MaxValue)

        val doubles: Array[java.lang.Double] = row(25).asInstanceOf[Array[java.lang.Double]]
        doubles(0) should be (Double.MaxValue - 2)
        doubles(1) should be (Double.MaxValue - 1)
        doubles(2) should be (Double.MaxValue)

        val localDates: Array[java.time.LocalDate] = row(26).asInstanceOf[Array[java.time.LocalDate]]
        localDates(0) should be (LocalDate.of(2019, 1, 1))
        localDates(1) should be (LocalDate.of(2019, 1, 2))
        localDates(2) should be (LocalDate.of(2019, 1, 3))

        val zoneDateTimes: Array[java.time.ZonedDateTime] = row(27).asInstanceOf[Array[java.time.ZonedDateTime]]
        zoneDateTimes(0) should be (ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC))
        zoneDateTimes(1) should be (ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC))
        zoneDateTimes(2) should be (ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC))

        val durations: Array[java.time.Duration] = row(28).asInstanceOf[Array[java.time.Duration]]
        durations(0) should be (java.time.Duration.ofSeconds(3600))
        durations(1) should be (java.time.Duration.ofSeconds(7200))
        durations(2) should be (java.time.Duration.ofSeconds(10800))

        val strings: Array[String] = row(29).asInstanceOf[Array[String]]
        strings(0) should be ("string1")
        strings(1) should be ("string2")
        strings(2) should be ("string3")

        val blobs: Array[Array[Byte]] = row(30).asInstanceOf[Array[Array[Byte]]]
        new String(blobs(0), StandardCharsets.UTF_8) should be ("blob1")
        new String(blobs(1), StandardCharsets.UTF_8) should be ("blob2")
        new String(blobs(2), StandardCharsets.UTF_8) should be ("blob3")

        val listOfStrings: Array[ByteGraphList] = row(31).asInstanceOf[Array[ByteGraphList]]
        listOfStrings(0).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string1", "string2", "string3"
        ))

        listOfStrings(1).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string4", "string5", "string6"
        ))

        listOfStrings(2).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string7", "string8", "string9"
        ))

        val dynamic = row(32).asInstanceOf[ByteGraphStructure]
        dynamic("field").get.as[String]() should be ("value")

        val reference: Array[AnyRef] = row(33).asInstanceOf[Array[AnyRef]]
        reference(0) should be (null)
        reference(1) should be (java.lang.Boolean.TRUE)
        reference(2) should be (java.lang.Boolean.FALSE)
        reference(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        reference(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        reference(5) should be (java.lang.Long.valueOf(Long.MaxValue))
      }

      "succeed reading a row with all field types using sql types" in withRow { byteGraphBytes =>
        val byteGraphReader = ByteGraph.reader(byteGraphRowSchema, useSqlTypes = true)
        val row = byteGraphReader.read(byteGraphBytes.bytes.bytes())

        row(0) should be (null)
        row(1) should be (java.lang.Boolean.TRUE)
        row(2) should be (java.lang.Boolean.FALSE)
        row(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        row(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        row(5) should be (java.lang.Long.valueOf(Long.MaxValue))

        java.lang.Short.toUnsignedInt(row(6).asInstanceOf[java.lang.Short]) should be (65535)
        java.lang.Integer.toUnsignedLong(row(7).asInstanceOf[Integer]) should be (4294967295L)
        java.lang.Long.toUnsignedString(row(8).asInstanceOf[java.lang.Long]) should be (
          "18446744073709551615"
        )

        row(9) should be (java.lang.Float.valueOf(Float.MaxValue))
        row(10) should be (java.lang.Double.valueOf(Double.MaxValue))
        row(11) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 1)))
        row(12) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(
          LocalDate.of(2019, 1, 1),
          LocalTime.of(23, 59, 59),
          ZoneOffset.UTC
        ).toLocalDateTime))

        row(13) should be (java.time.Duration.ofSeconds(3600).toMillis)
        row(14) should be ("string")
        new String(row(15).asInstanceOf[Array[Byte]], StandardCharsets.UTF_8) should be ("blob")

        val nested: Array[AnyRef] = row(16).asInstanceOf[Array[AnyRef]]
        nested(0) should be (null)
        nested(1) should be (java.lang.Boolean.TRUE)
        nested(2) should be (java.lang.Boolean.FALSE)
        nested(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        nested(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        nested(5) should be (java.lang.Long.valueOf(Long.MaxValue))

        val booleans: Array[java.lang.Boolean] = row(17).asInstanceOf[Array[java.lang.Boolean]]
        booleans(0) should be (java.lang.Boolean.TRUE)
        booleans(1) should be (java.lang.Boolean.FALSE)
        booleans(2) should be (java.lang.Boolean.TRUE)

        val integers: Array[java.lang.Integer] = row(18).asInstanceOf[Array[java.lang.Integer]]
        integers(0) should be (Int.MaxValue - 2)
        integers(1) should be (Int.MaxValue - 1)
        integers(2) should be (Int.MaxValue)

        val uIntegers: Array[java.lang.Integer] = row(19).asInstanceOf[Array[java.lang.Integer]]
        java.lang.Integer.toUnsignedLong(uIntegers(0)) should be (4294967293L)
        java.lang.Integer.toUnsignedLong(uIntegers(1)) should be (4294967294L)
        java.lang.Integer.toUnsignedLong(uIntegers(2)) should be (4294967295L)

        val shorts: Array[java.lang.Short] = row(20).asInstanceOf[Array[java.lang.Short]]
        shorts(0) should be (Short.MaxValue - 2)
        shorts(1) should be (Short.MaxValue - 1)
        shorts(2) should be (Short.MaxValue)

        val uShorts: Array[java.lang.Short] = row(21).asInstanceOf[Array[java.lang.Short]]
        java.lang.Short.toUnsignedInt(uShorts(0)) should be (65533)
        java.lang.Short.toUnsignedInt(uShorts(1)) should be (65534)
        java.lang.Short.toUnsignedInt(uShorts(2)) should be (65535)

        val longs: Array[java.lang.Long] = row(22).asInstanceOf[Array[java.lang.Long]]
        longs(0) should be (Long.MaxValue - 2)
        longs(1) should be (Long.MaxValue - 1)
        longs(2) should be (Long.MaxValue)

        val uLongs: Array[java.lang.Long] = row(23).asInstanceOf[Array[java.lang.Long]]
        java.lang.Long.toUnsignedString(uLongs(0)) should be ("18446744073709551613")
        java.lang.Long.toUnsignedString(uLongs(1)) should be ("18446744073709551614")
        java.lang.Long.toUnsignedString(uLongs(2)) should be ("18446744073709551615")

        val floats: Array[java.lang.Float] = row(24).asInstanceOf[Array[java.lang.Float]]
        floats(0) should be (Float.MaxValue - 2)
        floats(1) should be (Float.MaxValue - 1)
        floats(2) should be (Float.MaxValue)

        val doubles: Array[java.lang.Double] = row(25).asInstanceOf[Array[java.lang.Double]]
        doubles(0) should be (Double.MaxValue - 2)
        doubles(1) should be (Double.MaxValue - 1)
        doubles(2) should be (Double.MaxValue)

        val localDates: Array[java.sql.Date] = row(26).asInstanceOf[Array[java.sql.Date]]
        localDates(0) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 1)))
        localDates(1) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 2)))
        localDates(2) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 3)))

        val zoneDateTimes: Array[java.sql.Timestamp] = row(27).asInstanceOf[Array[java.sql.Timestamp]]
        zoneDateTimes(0) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC).toLocalDateTime))
        zoneDateTimes(1) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC).toLocalDateTime))
        zoneDateTimes(2) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC).toLocalDateTime))

        val durations: Array[java.lang.Long] = row(28).asInstanceOf[Array[java.lang.Long]]
        durations(0) should be (java.time.Duration.ofSeconds(3600).toMillis)
        durations(1) should be (java.time.Duration.ofSeconds(7200).toMillis)
        durations(2) should be (java.time.Duration.ofSeconds(10800).toMillis)

        val strings: Array[String] = row(29).asInstanceOf[Array[String]]
        strings(0) should be ("string1")
        strings(1) should be ("string2")
        strings(2) should be ("string3")

        val blobs: Array[Array[Byte]] = row(30).asInstanceOf[Array[Array[Byte]]]
        new String(blobs(0), StandardCharsets.UTF_8) should be ("blob1")
        new String(blobs(1), StandardCharsets.UTF_8) should be ("blob2")
        new String(blobs(2), StandardCharsets.UTF_8) should be ("blob3")

        val listOfStrings: Array[ByteGraphList] = row(31).asInstanceOf[Array[ByteGraphList]]
        listOfStrings(0).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string1", "string2", "string3"
        ))

        listOfStrings(1).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string4", "string5", "string6"
        ))

        listOfStrings(2).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string7", "string8", "string9"
        ))

        val dynamic = row(32).asInstanceOf[ByteGraphStructure]
        dynamic("field").get.as[String]() should be ("value")

        val reference: Array[AnyRef] = row(33).asInstanceOf[Array[AnyRef]]
        reference(0) should be (null)
        reference(1) should be (java.lang.Boolean.TRUE)
        reference(2) should be (java.lang.Boolean.FALSE)
        reference(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        reference(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        reference(5) should be (java.lang.Long.valueOf(Long.MaxValue))
      }

      "succeed converting a row to json" in withRow { byteBuf =>
        ByteGraph.toJsonString(byteBuf.as[ByteGraphRow]()) should be (
          """[null,true,false,32767,2147483647,9223372036854775807,-1,-1,-1,3.4028235E38,1.7976931348623157E308,"2019-01-01","2019-01-01T23:59:59Z","PT1H","string","YmxvYg==",[null,true,false,32767,2147483647,9223372036854775807],[true,false,true],[2147483645,2147483646,2147483647],[-3,-2,-1],[32765,32766,32767],[-3,-2,-1],[9223372036854775805,9223372036854775806,9223372036854775807],[-3,-2,-1],[3.4028235E38,3.4028235E38,3.4028235E38],[1.7976931348623157E308,1.7976931348623157E308,1.7976931348623157E308],["2019-01-01","2019-01-02","2019-01-03"],["2019-01-01T23:59:59Z","2019-01-02T23:59:59Z","2019-01-03T23:59:59Z"],["PT1H","PT2H","PT3H"],["string1","string2","string3"],["YmxvYjE=","YmxvYjI=","YmxvYjM="],[["string1","string2","string3"],["string4","string5","string6"],["string7","string8","string9"]],{"field":"value"},[null,true,false,32767,2147483647,9223372036854775807]]"""
        )
      }

      "succeed converting a row to a json document" in withRow { byteBuf =>
        ByteGraph.toJsonString(byteBuf.as[ByteGraphRow](), renderAsDocument = true) should be (
          """{"null":null,"true":true,"false":false,"short":32767,"int":2147483647,"long":9223372036854775807,"uShort":-1,"uInt":-1,"uLong":-1,"float":3.4028235E38,"double":1.7976931348623157E308,"date":"2019-01-01","dateTime":"2019-01-01T23:59:59Z","duration":"PT1H","string":"string","blob":"YmxvYg==","row":{"null":null,"true":true,"false":false,"short":32767,"int":2147483647,"long":9223372036854775807},"booleans":[true,false,true],"integers":[2147483645,2147483646,2147483647],"uIntegers":[-3,-2,-1],"shorts":[32765,32766,32767],"uShorts":[-3,-2,-1],"longs":[9223372036854775805,9223372036854775806,9223372036854775807],"uLongs":[-3,-2,-1],"floats":[3.4028235E38,3.4028235E38,3.4028235E38],"doubles":[1.7976931348623157E308,1.7976931348623157E308,1.7976931348623157E308],"dates":["2019-01-01","2019-01-02","2019-01-03"],"dateTimes":["2019-01-01T23:59:59Z","2019-01-02T23:59:59Z","2019-01-03T23:59:59Z"],"durations":["PT1H","PT2H","PT3H"],"strings":["string1","string2","string3"],"blobs":["YmxvYjE=","YmxvYjI=","YmxvYjM="],"listOfStrings":[["string1","string2","string3"],["string4","string5","string6"],["string7","string8","string9"]],"dynamic":{"field":"value"},"reference":{"null":null,"true":true,"false":false,"short":32767,"int":2147483647,"long":9223372036854775807}}"""
        )
      }

      "succeed reading a structure with all field types" in withStructure { byteGraphBytes =>
        val byteGraphReader = ByteGraph.reader(byteGraphStructureSchema)
        val structure = byteGraphReader.read(byteGraphBytes.bytes.bytes())

        structure(0) should be (null)
        structure(1) should be (java.lang.Boolean.TRUE)
        structure(2) should be (java.lang.Boolean.FALSE)
        structure(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        structure(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        structure(5) should be (java.lang.Long.valueOf(Long.MaxValue))

        java.lang.Short.toUnsignedInt(structure(6).asInstanceOf[java.lang.Short]) should be (65535)
        java.lang.Integer.toUnsignedLong(structure(7).asInstanceOf[Integer]) should be (4294967295L)
        java.lang.Long.toUnsignedString(structure(8).asInstanceOf[java.lang.Long]) should be (
          "18446744073709551615"
        )

        structure(9) should be (java.lang.Float.valueOf(Float.MaxValue))
        structure(10) should be (java.lang.Double.valueOf(Double.MaxValue))
        structure(11) should be (LocalDate.of(2019, 1, 1))
        structure(12) should be (ZonedDateTime.of(
          LocalDate.of(2019, 1, 1),
          LocalTime.of(23, 59, 59),
          ZoneOffset.UTC
        ))

        structure(13) should be (java.time.Duration.ofSeconds(3600))
        structure(14) should be ("string")
        new String(structure(15).asInstanceOf[Array[Byte]], StandardCharsets.UTF_8) should be ("blob")

        val nested: Array[AnyRef] = structure(16).asInstanceOf[Array[AnyRef]]
        nested(0) should be (null)
        nested(1) should be (java.lang.Boolean.TRUE)
        nested(2) should be (java.lang.Boolean.FALSE)
        nested(3) should be (java.lang.Short.valueOf(Short.MaxValue))
        nested(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
        nested(5) should be (java.lang.Long.valueOf(Long.MaxValue))

        val booleans: Array[java.lang.Boolean] = structure(17).asInstanceOf[Array[java.lang.Boolean]]
        booleans(0) should be (java.lang.Boolean.TRUE)
        booleans(1) should be (java.lang.Boolean.FALSE)
        booleans(2) should be (java.lang.Boolean.TRUE)

        val integers: Array[java.lang.Integer] = structure(18).asInstanceOf[Array[java.lang.Integer]]
        integers(0) should be (Int.MaxValue - 2)
        integers(1) should be (Int.MaxValue - 1)
        integers(2) should be (Int.MaxValue)

        val uIntegers: Array[java.lang.Integer] = structure(19).asInstanceOf[Array[java.lang.Integer]]
        java.lang.Integer.toUnsignedLong(uIntegers(0)) should be (4294967293L)
        java.lang.Integer.toUnsignedLong(uIntegers(1)) should be (4294967294L)
        java.lang.Integer.toUnsignedLong(uIntegers(2)) should be (4294967295L)

        val shorts: Array[java.lang.Short] = structure(20).asInstanceOf[Array[java.lang.Short]]
        shorts(0) should be (Short.MaxValue - 2)
        shorts(1) should be (Short.MaxValue - 1)
        shorts(2) should be (Short.MaxValue)

        val uShorts: Array[java.lang.Short] = structure(21).asInstanceOf[Array[java.lang.Short]]
        java.lang.Short.toUnsignedInt(uShorts(0)) should be (65533)
        java.lang.Short.toUnsignedInt(uShorts(1)) should be (65534)
        java.lang.Short.toUnsignedInt(uShorts(2)) should be (65535)

        val longs: Array[java.lang.Long] = structure(22).asInstanceOf[Array[java.lang.Long]]
        longs(0) should be (Long.MaxValue - 2)
        longs(1) should be (Long.MaxValue - 1)
        longs(2) should be (Long.MaxValue)

        val uLongs: Array[java.lang.Long] = structure(23).asInstanceOf[Array[java.lang.Long]]
        java.lang.Long.toUnsignedString(uLongs(0)) should be ("18446744073709551613")
        java.lang.Long.toUnsignedString(uLongs(1)) should be ("18446744073709551614")
        java.lang.Long.toUnsignedString(uLongs(2)) should be ("18446744073709551615")

        val floats: Array[java.lang.Float] = structure(24).asInstanceOf[Array[java.lang.Float]]
        floats(0) should be (Float.MaxValue - 2)
        floats(1) should be (Float.MaxValue - 1)
        floats(2) should be (Float.MaxValue)

        val doubles: Array[java.lang.Double] = structure(25).asInstanceOf[Array[java.lang.Double]]
        doubles(0) should be (Double.MaxValue - 2)
        doubles(1) should be (Double.MaxValue - 1)
        doubles(2) should be (Double.MaxValue)

        val localDates: Array[java.time.LocalDate] = structure(26).asInstanceOf[Array[java.time.LocalDate]]
        localDates(0) should be (LocalDate.of(2019, 1, 1))
        localDates(1) should be (LocalDate.of(2019, 1, 2))
        localDates(2) should be (LocalDate.of(2019, 1, 3))

        val zoneDateTimes: Array[java.time.ZonedDateTime] = structure(27).asInstanceOf[Array[java.time.ZonedDateTime]]
        zoneDateTimes(0) should be (ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC))
        zoneDateTimes(1) should be (ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC))
        zoneDateTimes(2) should be (ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC))

        val durations: Array[java.time.Duration] = structure(28).asInstanceOf[Array[java.time.Duration]]
        durations(0) should be (java.time.Duration.ofSeconds(3600))
        durations(1) should be (java.time.Duration.ofSeconds(7200))
        durations(2) should be (java.time.Duration.ofSeconds(10800))

        val strings: Array[String] = structure(29).asInstanceOf[Array[String]]
        strings(0) should be ("string1")
        strings(1) should be ("string2")
        strings(2) should be ("string3")

        val blobs: Array[Array[Byte]] = structure(30).asInstanceOf[Array[Array[Byte]]]
        new String(blobs(0), StandardCharsets.UTF_8) should be ("blob1")
        new String(blobs(1), StandardCharsets.UTF_8) should be ("blob2")
        new String(blobs(2), StandardCharsets.UTF_8) should be ("blob3")

        val listOfStrings: Array[ByteGraphList] = structure(31).asInstanceOf[Array[ByteGraphList]]
        listOfStrings(0).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string1", "string2", "string3"
        ))

        listOfStrings(1).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string4", "string5", "string6"
        ))

        listOfStrings(2).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
          "string7", "string8", "string9"
        ))
      }
    }

    "succeed reading a structure with all field types using sql types" in withStructure { byteGraphBytes =>
      val byteGraphReader = ByteGraph.reader(byteGraphStructureSchema, useSqlTypes = true)
      val structure = byteGraphReader.read(byteGraphBytes.bytes.bytes())

      structure(0) should be (null)
      structure(1) should be (java.lang.Boolean.TRUE)
      structure(2) should be (java.lang.Boolean.FALSE)
      structure(3) should be (java.lang.Short.valueOf(Short.MaxValue))
      structure(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
      structure(5) should be (java.lang.Long.valueOf(Long.MaxValue))

      java.lang.Short.toUnsignedInt(structure(6).asInstanceOf[java.lang.Short]) should be (65535)
      java.lang.Integer.toUnsignedLong(structure(7).asInstanceOf[Integer]) should be (4294967295L)
      java.lang.Long.toUnsignedString(structure(8).asInstanceOf[java.lang.Long]) should be (
        "18446744073709551615"
      )

      structure(9) should be (java.lang.Float.valueOf(Float.MaxValue))
      structure(10) should be (java.lang.Double.valueOf(Double.MaxValue))
      structure(11) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 1)))
      structure(12) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(
        LocalDate.of(2019, 1, 1),
        LocalTime.of(23, 59, 59),
        ZoneOffset.UTC
      ).toLocalDateTime))

      structure(13) should be (java.time.Duration.ofSeconds(3600).toMillis)
      structure(14) should be ("string")
      new String(structure(15).asInstanceOf[Array[Byte]], StandardCharsets.UTF_8) should be ("blob")

      val nested: Array[AnyRef] = structure(16).asInstanceOf[Array[AnyRef]]
      nested(0) should be (null)
      nested(1) should be (java.lang.Boolean.TRUE)
      nested(2) should be (java.lang.Boolean.FALSE)
      nested(3) should be (java.lang.Short.valueOf(Short.MaxValue))
      nested(4) should be (java.lang.Integer.valueOf(Int.MaxValue))
      nested(5) should be (java.lang.Long.valueOf(Long.MaxValue))

      val booleans: Array[java.lang.Boolean] = structure(17).asInstanceOf[Array[java.lang.Boolean]]
      booleans(0) should be (java.lang.Boolean.TRUE)
      booleans(1) should be (java.lang.Boolean.FALSE)
      booleans(2) should be (java.lang.Boolean.TRUE)

      val integers: Array[java.lang.Integer] = structure(18).asInstanceOf[Array[java.lang.Integer]]
      integers(0) should be (Int.MaxValue - 2)
      integers(1) should be (Int.MaxValue - 1)
      integers(2) should be (Int.MaxValue)

      val uIntegers: Array[java.lang.Integer] = structure(19).asInstanceOf[Array[java.lang.Integer]]
      java.lang.Integer.toUnsignedLong(uIntegers(0)) should be (4294967293L)
      java.lang.Integer.toUnsignedLong(uIntegers(1)) should be (4294967294L)
      java.lang.Integer.toUnsignedLong(uIntegers(2)) should be (4294967295L)

      val shorts: Array[java.lang.Short] = structure(20).asInstanceOf[Array[java.lang.Short]]
      shorts(0) should be (Short.MaxValue - 2)
      shorts(1) should be (Short.MaxValue - 1)
      shorts(2) should be (Short.MaxValue)

      val uShorts: Array[java.lang.Short] = structure(21).asInstanceOf[Array[java.lang.Short]]
      java.lang.Short.toUnsignedInt(uShorts(0)) should be (65533)
      java.lang.Short.toUnsignedInt(uShorts(1)) should be (65534)
      java.lang.Short.toUnsignedInt(uShorts(2)) should be (65535)

      val longs: Array[java.lang.Long] = structure(22).asInstanceOf[Array[java.lang.Long]]
      longs(0) should be (Long.MaxValue - 2)
      longs(1) should be (Long.MaxValue - 1)
      longs(2) should be (Long.MaxValue)

      val uLongs: Array[java.lang.Long] = structure(23).asInstanceOf[Array[java.lang.Long]]
      java.lang.Long.toUnsignedString(uLongs(0)) should be ("18446744073709551613")
      java.lang.Long.toUnsignedString(uLongs(1)) should be ("18446744073709551614")
      java.lang.Long.toUnsignedString(uLongs(2)) should be ("18446744073709551615")

      val floats: Array[java.lang.Float] = structure(24).asInstanceOf[Array[java.lang.Float]]
      floats(0) should be (Float.MaxValue - 2)
      floats(1) should be (Float.MaxValue - 1)
      floats(2) should be (Float.MaxValue)

      val doubles: Array[java.lang.Double] = structure(25).asInstanceOf[Array[java.lang.Double]]
      doubles(0) should be (Double.MaxValue - 2)
      doubles(1) should be (Double.MaxValue - 1)
      doubles(2) should be (Double.MaxValue)

      val localDates: Array[java.sql.Date] = structure(26).asInstanceOf[Array[java.sql.Date]]
      localDates(0) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 1)))
      localDates(1) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 2)))
      localDates(2) should be (java.sql.Date.valueOf(LocalDate.of(2019, 1, 3)))

      val zoneDateTimes: Array[java.sql.Timestamp] = structure(27).asInstanceOf[Array[java.sql.Timestamp]]
      zoneDateTimes(0) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(LocalDate.of(2019, 1, 1), LocalTime.of(23, 59, 59), ZoneOffset.UTC).toLocalDateTime))
      zoneDateTimes(1) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(LocalDate.of(2019, 1, 2), LocalTime.of(23, 59, 59), ZoneOffset.UTC).toLocalDateTime))
      zoneDateTimes(2) should be (java.sql.Timestamp.valueOf(ZonedDateTime.of(LocalDate.of(2019, 1, 3), LocalTime.of(23, 59, 59), ZoneOffset.UTC).toLocalDateTime))

      val durations: Array[java.lang.Long] = structure(28).asInstanceOf[Array[java.lang.Long]]
      durations(0) should be (java.time.Duration.ofSeconds(3600).toMillis)
      durations(1) should be (java.time.Duration.ofSeconds(7200).toMillis)
      durations(2) should be (java.time.Duration.ofSeconds(10800).toMillis)

      val strings: Array[String] = structure(29).asInstanceOf[Array[String]]
      strings(0) should be ("string1")
      strings(1) should be ("string2")
      strings(2) should be ("string3")

      val blobs: Array[Array[Byte]] = structure(30).asInstanceOf[Array[Array[Byte]]]
      new String(blobs(0), StandardCharsets.UTF_8) should be ("blob1")
      new String(blobs(1), StandardCharsets.UTF_8) should be ("blob2")
      new String(blobs(2), StandardCharsets.UTF_8) should be ("blob3")

      val listOfStrings: Array[ByteGraphList] = structure(31).asInstanceOf[Array[ByteGraphList]]
      listOfStrings(0).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
        "string1", "string2", "string3"
      ))

      listOfStrings(1).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
        "string4", "string5", "string6"
      ))

      listOfStrings(2).as[ByteGraphList]().iterator.map(_.as[String]()).toList should be (List(
        "string7", "string8", "string9"
      ))
    }

    "iterating on a list" should {
      "succeed reading all values" in withList { byteBuf =>
        ByteGraph.toJsonString(byteBuf.as[ByteGraphList]()) should be (
          """[null,true,false,32767,2147483647,9223372036854775807,-1,-1,-1,3.4028235E38,1.7976931348623157E308,"2019-01-01","2019-01-01T23:59:59Z","PT1H","string","YmxvYg==",[true,false,true],[2147483645,2147483646,2147483647],[-3,-2,-1],[32765,32766,32767],[-3,-2,-1],[9223372036854775805,9223372036854775806,9223372036854775807],[-3,-2,-1],[3.4028235E38,3.4028235E38,3.4028235E38],[1.7976931348623157E308,1.7976931348623157E308,1.7976931348623157E308],["2019-01-01","2019-01-02","2019-01-03"],["2019-01-01T23:59:59Z","2019-01-02T23:59:59Z","2019-01-03T23:59:59Z"],["PT1H","PT2H","PT3H"],["string1","string2","string3"],["YmxvYjE=","YmxvYjI=","YmxvYjM="],[["string1","string2","string3"],["string4","string5","string6"],["string7","string8","string9"]]]"""
        )
      }
    }

    "iterating on a structure" should {
      "succeed reading all values" in withStructure { byteBuf =>
        ByteGraph.toJsonString(byteBuf.as[ByteGraphStructure]()) should be (
          """{"null":null,"true":true,"false":false,"short":32767,"int":2147483647,"long":9223372036854775807,"uShort":-1,"uInt":-1,"uLong":-1,"float":3.4028235E38,"double":1.7976931348623157E308,"date":"2019-01-01","dateTime":"2019-01-01T23:59:59Z","duration":"PT1H","string":"string","blob":"YmxvYg==","structure":{"null":null,"true":true,"false":false,"short":32767,"int":2147483647,"long":9223372036854775807},"booleans":[true,false,true],"integers":[2147483645,2147483646,2147483647],"uIntegers":[-3,-2,-1],"shorts":[32765,32766,32767],"uShorts":[-3,-2,-1],"longs":[9223372036854775805,9223372036854775806,9223372036854775807],"uLongs":[-3,-2,-1],"floats":[3.4028235E38,3.4028235E38,3.4028235E38],"doubles":[1.7976931348623157E308,1.7976931348623157E308,1.7976931348623157E308],"dates":["2019-01-01","2019-01-02","2019-01-03"],"dateTimes":["2019-01-01T23:59:59Z","2019-01-02T23:59:59Z","2019-01-03T23:59:59Z"],"durations":["PT1H","PT2H","PT3H"],"strings":["string1","string2","string3"],"blobs":["YmxvYjE=","YmxvYjI=","YmxvYjM="],"listOfStrings":[["string1","string2","string3"],["string4","string5","string6"],["string7","string8","string9"]]}"""
        )
      }
    }

  }

}
