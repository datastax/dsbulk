/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.format.row;

import static com.datastax.oss.dsbulk.format.row.RowFormatter.UNLIMITED;
import static com.datastax.oss.dsbulk.format.row.RowFormatterSymbols.lineSeparator;
import static com.datastax.oss.dsbulk.format.row.RowFormatterSymbols.nameValueSeparator;
import static com.datastax.oss.dsbulk.format.row.RowFormatterSymbols.nullValue;
import static com.datastax.oss.dsbulk.format.row.RowFormatterSymbols.truncatedOutput;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;

/**
 * This class exposes utility methods to help in formatting a row.
 *
 * <p>Instances of this class are designed to format one single row; they keep internal counters
 * such as the current number of printed bound values, and for this reason, they should not be
 * reused to format more than one row.
 *
 * <p>This class is NOT thread-safe.
 */
public final class RowWriter implements Appendable {

  private static final int MAX_EXCEEDED = -1;

  private final StringBuilder buffer;
  private final int indentation;
  private final int maxValueLength;
  private final int maxValues;
  private final ProtocolVersion protocolVersion;
  private final CodecRegistry codecRegistry;
  private int remainingValues;

  RowWriter(
      StringBuilder buffer,
      int indentation,
      int maxValueLength,
      int maxValues,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry) {
    this.buffer = buffer;
    this.indentation = indentation;
    this.maxValueLength = maxValueLength;
    this.maxValues = maxValues;
    this.protocolVersion = protocolVersion;
    this.codecRegistry = codecRegistry;
    remainingValues = maxValues == UNLIMITED ? Integer.MAX_VALUE : maxValues;
  }

  /** @return The protocol version in use. */
  public ProtocolVersion getProtocolVersion() {
    return protocolVersion;
  }

  /** @return The codec registry version in use. */
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  /**
   * @return {@code true} if the maximum number of bound values per row is exceeded, {@code false}
   *     otherwise.
   */
  public boolean maxAppendedValuesExceeded() {
    return remainingValues == MAX_EXCEEDED;
  }

  public void newLine() {
    buffer.append(lineSeparator);
  }

  public void indent() {
    for (int i = 0; i < indentation; i++) {
      buffer.append(' ');
    }
  }

  @Override
  public RowWriter append(CharSequence csq) {
    buffer.append(csq);
    return this;
  }

  @Override
  public RowWriter append(CharSequence csq, int start, int end) {
    buffer.append(csq, start, end);
    return this;
  }

  @Override
  public RowWriter append(char c) {
    buffer.append(c);
    return this;
  }

  public RowWriter append(Object obj) {
    buffer.append(obj);
    return this;
  }

  public RowWriter append(String str) {
    buffer.append(str);
    return this;
  }

  /**
   * Appends the row's class name and hash code, as done by {@link Object#toString()}.
   *
   * @param row The row to format.
   * @return this (for method chaining).
   */
  public RowWriter appendClassNameAndHashCode(Row row) {
    String fqcn = row.getClass().getName();
    buffer.append(fqcn);
    buffer.append('@');
    buffer.append(Integer.toHexString(row.hashCode()));
    return this;
  }

  public void appendValue(String name, Object value, DataType type) {
    if (maxAppendedValuesExceeded()) return;
    if (value == null) {
      doAppendValue(name, nullValue);
      return;
    } else if (value instanceof ByteBuffer && maxValueLength != UNLIMITED) {
      ByteBuffer byteBuffer = (ByteBuffer) value;
      int maxBufferLengthInBytes = Math.max(2, maxValueLength / 2) - 1;
      boolean bufferLengthExceeded = byteBuffer.remaining() > maxBufferLengthInBytes;
      // prevent large blobs from being converted to strings
      if (bufferLengthExceeded) {
        byteBuffer = byteBuffer.duplicate();
        byteBuffer.limit(maxBufferLengthInBytes);
        // force usage of blob codec as any other codec would probably fail to format
        // a cropped byte buffer anyway
        String formatted = TypeCodecs.BLOB.format(byteBuffer);
        doAppendValue(name, formatted);
        buffer.append(truncatedOutput);
        return;
      }
    }
    TypeCodec<Object> codec =
        type == null ? codecRegistry.codecFor(value) : codecRegistry.codecFor(type, value);
    doAppendValue(name, codec.format(value));
  }

  private void doAppendValue(String name, String value) {
    if (maxAppendedValuesExceeded()) return;
    if (remainingValues == 0) {
      buffer.append(truncatedOutput);
      remainingValues = MAX_EXCEEDED;
      return;
    }
    boolean lengthExceeded = false;
    if (maxValueLength != UNLIMITED && value.length() > maxValueLength) {
      value = value.substring(0, maxValueLength);
      lengthExceeded = true;
    }
    if (name != null) {
      buffer.append(name);
      buffer.append(nameValueSeparator);
    }
    buffer.append(value);
    if (lengthExceeded) buffer.append(truncatedOutput);
    if (maxValues != UNLIMITED) remainingValues--;
  }

  @Override
  public String toString() {
    return buffer.toString();
  }
}
