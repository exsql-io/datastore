package io.exsql.bytegraph;

/*
 * Copyright (C) 2005-2017 Sebastiano Vigna
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.MeasurableOutputStream;
import it.unimi.dsi.fastutil.io.RepositionableStream;

/** Simple, fast byte-array output stream that exposes the backing array.
 *
 * <p>{@link java.io.ByteArrayOutputStream} is nice, but to get its content you
 * must generate each time a new object. This doesn't happen here.
 *
 * @author Sebastiano Vigna
 */

public class FasterByteArrayOutputStream extends MeasurableOutputStream implements RepositionableStream {

	/** The array backing the output stream. */
	private static final int DEFAULT_INITIAL_CAPACITY = 256;

	/** The array backing the output stream. */
	public byte[] array;

	/** The number of valid bytes in {@link #array}. */
	public int length;

	/** The current writing position. */
	private int position;

	/** Creates a new array output stream with an initial capacity of {@link #DEFAULT_INITIAL_CAPACITY} bytes. */
	public FasterByteArrayOutputStream() {
        array = new byte[DEFAULT_INITIAL_CAPACITY];
	}

	public FasterByteArrayOutputStream(final int initialCapacity) {
		array = new byte[initialCapacity];
	}

	public byte[] bytes() {
		return ByteArrays.trim(array, length);
	}

	@Override
	public void write(final int b) {
		if (position >= array.length) array = ByteArrays.grow(array, position + 1, length);
		array[position++] = (byte)b;
		if (length < position) length = position;
	}

	public void writeShort(final short s) {
		if (position + Short.BYTES >= array.length) array = ByteArrays.grow(array, position + Short.BYTES, length);
		array[position++] = (byte)(s >>> 8 & 255);
		array[position++] = (byte)(s & 255);
		if (length < position) length = position;
	}

	public void writeInt(final int i) {
		if (position + Integer.BYTES >= array.length) array = ByteArrays.grow(array, position + Integer.BYTES, length);
		array[position++] = (byte)(i >>> 24 & 255);
		array[position++] = (byte)(i >>> 16 & 255);
		array[position++] = (byte)(i >>> 8 & 255);
		array[position++] = (byte)(i & 255);
		if (length < position) length = position;
	}

	public void writeInt(final int offset, final int i) {
		array[offset] = (byte)(i >>> 24 & 255);
		array[offset + 1] = (byte)(i >>> 16 & 255);
		array[offset + 2] = (byte)(i >>> 8 & 255);
		array[offset + 3] = (byte)(i & 255);
	}

    public void writeLong(final long l) {
        if (position + Long.BYTES >= array.length) array = ByteArrays.grow(array, position + Long.BYTES, length);
        array[position++] = (byte)(l >>> 56 & 255);
        array[position++] = (byte)(l >>> 48 & 255);
        array[position++] = (byte)(l >>> 40 & 255);
        array[position++] = (byte)(l >>> 32 & 255);
        array[position++] = (byte)(l >>> 24 & 255);
        array[position++] = (byte)(l >>> 16 & 255);
        array[position++] = (byte)(l >>> 8 & 255);
        array[position++] = (byte)(l & 255);
        if (length < position) length = position;
    }

	@Override
	public void write(final byte[] b, final int off, final int len) {
		if (position + len > array.length) array = ByteArrays.grow(array, position + len, position);
		System.arraycopy(b, off, array, position, len);
		if (position + len > length) length = position += len;
	}

	@Override
	public void position(long newPosition) {
		if (newPosition > Integer.MAX_VALUE) throw new IllegalArgumentException("Position too large: " + newPosition);
		position = (int)newPosition;
	}

	@Override
	public long position() {
		return position;
	}

	@Override
	public long length() {
		return length;
	}

}
