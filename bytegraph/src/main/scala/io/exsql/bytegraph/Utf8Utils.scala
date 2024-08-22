package io.exsql.bytegraph

import io.exsql.bytegraph.bytes.ByteGraphBytes

import java.util

object Utf8Utils {

  @inline final def encode(string: String): Array[Byte] = {
    encodeUtf8(string)
  }

  @inline final def encode(string: String, bytes: ByteGraphBytes): Int = {
    val encoded = encodeUtf8(string)
    bytes.write(encoded)
    encoded.length
  }

  @inline final def decode(bytes: Array[Byte]): String = {
    Utf8.decodeUtf8(bytes, 0, bytes.length)
  }

  @inline final def decode(bytes: Array[Byte], start: Int, length: Int): String = {
    Utf8.decodeUtf8(bytes, start, length)
  }

  @inline final private def encodeUtf8(string: String): Array[Byte] = {
    val buffer = Array.ofDim[Byte](string.length * Utf8.MAX_BYTES_PER_CHAR)
    val size = Utf8.encode(string, buffer, 0, buffer.length)
    util.Arrays.copyOfRange(buffer, 0, size)
  }

}
