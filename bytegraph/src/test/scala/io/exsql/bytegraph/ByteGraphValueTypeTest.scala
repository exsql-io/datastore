package io.exsql.bytegraph

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

class ByteGraphValueTypeTest extends AnyWordSpec with Matchers {

  "A ByteGraphValueType" when {
    "read from a byte" should {
      "yield a null type" in {
        ByteGraphValueType.readValueTypeFromByte(0x0F) should be (ByteGraphValueType.Null)
      }

      "yield a boolean type" in {
        ByteGraphValueType.readValueTypeFromByte(0x10) should be (ByteGraphValueType.Boolean)
      }

      "yield a short type" in {
        ByteGraphValueType.readValueTypeFromByte(0x12) should be (ByteGraphValueType.Short)
      }

      "yield a int type" in {
        ByteGraphValueType.readValueTypeFromByte(0x14) should be (ByteGraphValueType.Int)
      }

      "yield a long type" in {
        ByteGraphValueType.readValueTypeFromByte(0x18) should be (ByteGraphValueType.Long)
      }

      "yield a unsigned short type" in {
        ByteGraphValueType.readValueTypeFromByte(0x22) should be (ByteGraphValueType.UShort)
      }

      "yield a unsigned int type" in {
        ByteGraphValueType.readValueTypeFromByte(0x24) should be (ByteGraphValueType.UInt)
      }

      "yield a unsigned long type" in {
        ByteGraphValueType.readValueTypeFromByte(0x28) should be (ByteGraphValueType.ULong)
      }

      "yield a float type" in {
        ByteGraphValueType.readValueTypeFromByte(0x34) should be (ByteGraphValueType.Float)
      }

      "yield a double type" in {
        ByteGraphValueType.readValueTypeFromByte(0x38) should be (ByteGraphValueType.Double)
      }

      "yield a date type" in {
        ByteGraphValueType.readValueTypeFromByte(0x43) should be (ByteGraphValueType.Date)
      }

      "yield a datetime type" in {
        ByteGraphValueType.readValueTypeFromByte(0x48) should be (ByteGraphValueType.DateTime)
      }

      "yield a duration type" in {
        ByteGraphValueType.readValueTypeFromByte(0x58) should be (ByteGraphValueType.Duration)
      }

      "yield a reference type" in {
        ByteGraphValueType.readValueTypeFromByte(0x64) should be (ByteGraphValueType.Reference)
      }

      "yield a string type" in {
        ByteGraphValueType.readValueTypeFromByte(0x7F) should be (ByteGraphValueType.String)
      }

      "yield a blob type" in {
        ByteGraphValueType.readValueTypeFromByte(0x8F.toByte) should be (ByteGraphValueType.Blob)
      }

      "yield a list type" in {
        ByteGraphValueType.readValueTypeFromByte(0x9F.toByte) should be (ByteGraphValueType.List)
      }

      "yield a structure type" in {
        ByteGraphValueType.readValueTypeFromByte(0xAF.toByte) should be (ByteGraphValueType.Structure)
      }

      "yield a row type" in {
        ByteGraphValueType.readValueTypeFromByte(0xBF.toByte) should be (ByteGraphValueType.Row)
      }

      "yield a symbol type" in {
        ByteGraphValueType.readValueTypeFromByte(0xC0.toByte) should be (ByteGraphValueType.Symbol)
      }

      "yield a unknown type" in {
        ByteGraphValueType.readValueTypeFromByte(0xD0.toByte) should be (ByteGraphValueType.Unknown)
      }

      "yield a unknown type when the flag is signed family but the length is not supported" in {
        ByteGraphValueType.readValueTypeFromByte(0x15) should be (ByteGraphValueType.Unknown)
      }

      "yield a unknown type when the flag is floating point family but the length is not supported" in {
        ByteGraphValueType.readValueTypeFromByte(0x35) should be (ByteGraphValueType.Unknown)
      }

      "yield a unknown type when the flag is time family but the length is not supported" in {
        ByteGraphValueType.readValueTypeFromByte(0x45) should be (ByteGraphValueType.Unknown)
      }

      "yield a unknown type when the flag is duration but the length is not supported" in {
        ByteGraphValueType.readValueTypeFromByte(0x55) should be (ByteGraphValueType.Unknown)
      }

      "yield a unknown type when the flag is reference but the length is not supported" in {
        ByteGraphValueType.readValueTypeFromByte(0x65) should be (ByteGraphValueType.Unknown)
      }
    }
  }

}
