/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ery.ertc.estorm.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Utility class with methods for manipulating Writable objects
 */
public abstract class Writables {
	abstract void write(DataOutput out) throws IOException;

	abstract void readFields(DataInput in) throws IOException;

	/**
	 * @param w
	 *            writable
	 * @return The bytes of <code>w</code> gotten by running its {@link Writable#write(java.io.DataOutput)} method.
	 * @throws IOException
	 *             e
	 * @see #getWritable(byte[], Writable)
	 */
	public static byte[] getBytes(final Writables w) throws IOException {
		if (w == null) {
			throw new IllegalArgumentException("Writable cannot be null");
		}
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(byteStream);
		try {
			w.write(out);
			out.close();
			out = null;
			return byteStream.toByteArray();
		} finally {
			if (out != null) {
				out.close();
			}
		}
	}

	/**
	 * Put a bunch of Writables as bytes all into the one byte array.
	 * 
	 * @param ws
	 *            writable
	 * @return The bytes of <code>w</code> gotten by running its {@link Writable#write(java.io.DataOutput)} method.
	 * @throws IOException
	 *             e
	 */
	public static byte[] getBytes(final Writables... ws) throws IOException {
		List<byte[]> bytes = new ArrayList<byte[]>();
		int size = 0;
		for (Writables w : ws) {
			byte[] b = getBytes(w);
			size += b.length;
			bytes.add(b);
		}
		byte[] result = new byte[size];
		int offset = 0;
		for (byte[] b : bytes) {
			System.arraycopy(b, 0, result, offset, b.length);
			offset += b.length;
		}
		return result;
	}

	/**
	 * Set bytes into the passed Writable by calling its {@link Writable#readFields(java.io.DataInput)}.
	 * 
	 * @param bytes
	 *            serialized bytes
	 * @param w
	 *            An empty Writable (usually made by calling the null-arg constructor).
	 * @return The passed Writable after its readFields has been called fed by the passed <code>bytes</code> array or
	 *         IllegalArgumentException if passed null or an empty <code>bytes</code> array.
	 * @throws IOException
	 *             e
	 * @throws IllegalArgumentException
	 */
	public static Writables getWritable(final byte[] bytes, final Writables w) throws IOException {
		return getWritable(bytes, 0, bytes.length, w);
	}

	/**
	 * Set bytes into the passed Writable by calling its {@link Writable#readFields(java.io.DataInput)}.
	 * 
	 * @param bytes
	 *            serialized bytes
	 * @param offset
	 *            offset into array
	 * @param length
	 *            length of data
	 * @param w
	 *            An empty Writable (usually made by calling the null-arg constructor).
	 * @return The passed Writable after its readFields has been called fed by the passed <code>bytes</code> array or
	 *         IllegalArgumentException if passed null or an empty <code>bytes</code> array.
	 * @throws IOException
	 *             e
	 * @throws IllegalArgumentException
	 */
	public static Writables getWritable(final byte[] bytes, final int offset, final int length, final Writables w) throws IOException {
		if (bytes == null || length <= 0) {
			throw new IllegalArgumentException("Can't build a writable with empty " + "bytes array");
		}
		if (w == null) {
			throw new IllegalArgumentException("Writable cannot be null");
		}
		DataInputBuffer in = new DataInputBuffer();
		try {
			in.reset(bytes, offset, length);
			w.readFields(in);
			return w;
		} finally {
			in.close();
		}
	}

	/**
	 * Copy one Writable to another. Copies bytes using data streams.
	 * 
	 * @param src
	 *            Source Writable
	 * @param tgt
	 *            Target Writable
	 * @return The target Writable.
	 * @throws IOException
	 *             e
	 */
	public static Writables copyWritable(final Writables src, final Writables tgt) throws IOException {
		return copyWritable(getBytes(src), tgt);
	}

	/**
	 * Copy one Writable to another. Copies bytes using data streams.
	 * 
	 * @param bytes
	 *            Source Writable
	 * @param tgt
	 *            Target Writable
	 * @return The target Writable.
	 * @throws IOException
	 *             e
	 */
	public static Writables copyWritable(final byte[] bytes, final Writables tgt) throws IOException {
		DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
		try {
			tgt.readFields(dis);
		} finally {
			dis.close();
		}
		return tgt;
	}
}
