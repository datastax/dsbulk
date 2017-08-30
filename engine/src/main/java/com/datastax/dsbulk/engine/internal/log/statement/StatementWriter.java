/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterLimits.UNLIMITED;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.lineSeparator;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.nameValueSeparator;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.nullValue;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.truncatedOutput;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterSymbols.unsetValue;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.log.LogUtils;
import java.nio.ByteBuffer;

/**
 * This class exposes utility methods to help {@link StatementPrinter statement printers} in
 * formatting a statement.
 *
 * <p>Instances of this class are designed to format one single statement; they keep internal
 * counters such as the current number of printed bound values, and for this reason, they should not
 * be reused to format more than one statement. When formatting more than one statement (e.g. when
 * formatting a {@link BatchStatement} and its children), one should call {@link
 * #createChildWriter()} to create child instances of the main writer to format each individual
 * statement.
 *
 * <p>This class is NOT thread-safe.
 */
public final class StatementWriter implements Appendable {

  private static final int MAX_EXCEEDED = -2;

  private final StringBuilder buffer;
  private final int indentation;
  private final StatementPrinterRegistry printerRegistry;
  private final StatementFormatterLimits limits;
  private final ProtocolVersion protocolVersion;
  private final CodecRegistry codecRegistry;
  private int remainingQueryStringChars;
  private int remainingBoundValues;

  StatementWriter(
      StringBuilder buffer,
      int indentation,
      StatementPrinterRegistry printerRegistry,
      StatementFormatterLimits limits,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry) {
    this.buffer = buffer;
    this.indentation = indentation;
    this.printerRegistry = printerRegistry;
    this.limits = limits;
    this.protocolVersion = protocolVersion;
    this.codecRegistry = codecRegistry;
    remainingQueryStringChars =
        limits.maxQueryStringLength == UNLIMITED ? Integer.MAX_VALUE : limits.maxQueryStringLength;
    remainingBoundValues =
        limits.maxBoundValues == UNLIMITED ? Integer.MAX_VALUE : limits.maxBoundValues;
  }

  /**
   * Creates and returns a child {@link StatementWriter}.
   *
   * <p>A child writer shares the same buffer as its parent, but has its own independent state. It
   * is most useful when dealing with inner statements in batches (each inner statement should use a
   * child writer).
   *
   * @return a child {@link StatementWriter}.
   */
  public StatementWriter createChildWriter() {
    return new StatementWriter(
        buffer, indentation + 4, printerRegistry, limits, protocolVersion, codecRegistry);
  }

  /** @return The {@link StatementPrinterRegistry printer registry}. */
  public StatementPrinterRegistry getPrinterRegistry() {
    return printerRegistry;
  }

  /** @return The current limits. */
  public StatementFormatterLimits getLimits() {
    return limits;
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
   * @return {@code true} if the maximum query string length is exceeded, {@code false} otherwise.
   */
  public boolean maxQueryStringLengthExceeded() {
    return remainingQueryStringChars == MAX_EXCEEDED;
  }

  /**
   * @return {@code true} if the maximum number of bound values per statement is exceeded, {@code
   *     false} otherwise.
   */
  public boolean maxAppendedBoundValuesExceeded() {
    return remainingBoundValues == MAX_EXCEEDED;
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
  public StatementWriter append(CharSequence csq) {
    buffer.append(csq);
    return this;
  }

  @Override
  public StatementWriter append(CharSequence csq, int start, int end) {
    buffer.append(csq, start, end);
    return this;
  }

  @Override
  public StatementWriter append(char c) {
    buffer.append(c);
    return this;
  }

  public StatementWriter append(Object obj) {
    buffer.append(obj);
    return this;
  }

  public StatementWriter append(String str) {
    buffer.append(str);
    return this;
  }

  /**
   * Appends the statement's class name and hash code, as done by {@link Object#toString()}.
   *
   * @param statement The statement to format.
   * @return this (for method chaining).
   */
  public StatementWriter appendClassNameAndHashCode(Statement statement) {
    String fqcn = statement.getClass().getName();
    if (fqcn.startsWith("com.datastax.driver.core.querybuilder")) fqcn = "BuiltStatement";
    if (fqcn.startsWith("com.datastax.driver.core.schemabuilder")) fqcn = "SchemaStatement";
    else if (fqcn.startsWith("com.datastax.driver.core.")) fqcn = fqcn.substring(25);
    else if (fqcn.startsWith("com.datastax.dsbulk.executor.api.statement."))
      fqcn = fqcn.substring(43);
    buffer.append(fqcn);
    buffer.append('@');
    buffer.append(Integer.toHexString(statement.hashCode()));
    return this;
  }

  /**
   * Appends the given fragment as a query string fragment.
   *
   * <p>This method can be called multiple times, in case the printer needs to compute the query
   * string by pieces.
   *
   * <p>This methods also keeps track of the amount of characters used so far to print the query
   * string, and automatically detects when the query string exceeds {@link
   * StatementFormatterLimits#maxQueryStringLength the maximum length}, in which case it truncates
   * the output.
   *
   * @param queryStringFragment The query string fragment to append
   */
  public void appendQueryStringFragment(String queryStringFragment) {
    if (!maxQueryStringLengthExceeded() && !queryStringFragment.isEmpty()) {
      if (limits.maxQueryStringLength == UNLIMITED) buffer.append(queryStringFragment);
      else if (queryStringFragment.length() > remainingQueryStringChars) {
        if (remainingQueryStringChars > 0) {
          queryStringFragment = queryStringFragment.substring(0, remainingQueryStringChars);
          buffer.append(queryStringFragment);
        }
        buffer.append(truncatedOutput);
        remainingQueryStringChars = MAX_EXCEEDED;
      } else {
        buffer.append(queryStringFragment);
        remainingQueryStringChars -= queryStringFragment.length();
      }
    }
  }

  public void appendBoundValue(int index, Object value, DataType type) {
    if (maxAppendedBoundValuesExceeded()) return;
    appendBoundValue(Integer.toString(index), value, type);
  }

  public void appendBoundValue(String name, Object value, DataType type) {
    if (maxAppendedBoundValuesExceeded()) return;
    if (value == null) {
      doAppendBoundValue(name, nullValue);
      return;
    } else if (value instanceof ByteBuffer && limits.maxBoundValueLength != UNLIMITED) {
      ByteBuffer byteBuffer = (ByteBuffer) value;
      int maxBufferLengthInBytes = Math.max(2, limits.maxBoundValueLength / 2) - 1;
      boolean bufferLengthExceeded = byteBuffer.remaining() > maxBufferLengthInBytes;
      // prevent large blobs from being converted to strings
      if (bufferLengthExceeded) {
        byteBuffer = (ByteBuffer) byteBuffer.duplicate().limit(maxBufferLengthInBytes);
        // force usage of blob codec as any other codec would probably fail to format
        // a cropped byte buffer anyway
        String formatted = TypeCodec.blob().format(byteBuffer);
        doAppendBoundValue(name, formatted);
        buffer.append(truncatedOutput);
        return;
      }
    }
    TypeCodec<Object> codec =
        type == null ? codecRegistry.codecFor(value) : codecRegistry.codecFor(type, value);
    doAppendBoundValue(name, codec.format(value));
  }

  public void appendUnsetBoundValue(String name) {
    doAppendBoundValue(name, unsetValue);
  }

  public void appendRecord(Record record) {
    LogUtils.appendRecordInfo(
        record,
        (o) -> {
          newLine();
          indent();
          buffer.append(o);
        });
  }

  private void doAppendBoundValue(String name, String value) {
    if (maxAppendedBoundValuesExceeded()) return;
    if (remainingBoundValues == 0) {
      buffer.append(truncatedOutput);
      remainingBoundValues = MAX_EXCEEDED;
      return;
    }
    boolean lengthExceeded = false;
    if (limits.maxBoundValueLength != UNLIMITED && value.length() > limits.maxBoundValueLength) {
      value = value.substring(0, limits.maxBoundValueLength);
      lengthExceeded = true;
    }
    if (name != null) {
      buffer.append(name);
      buffer.append(nameValueSeparator);
    }
    buffer.append(value);
    if (lengthExceeded) buffer.append(truncatedOutput);
    if (limits.maxBoundValues != UNLIMITED) remainingBoundValues--;
  }

  @Override
  public String toString() {
    return buffer.toString();
  }
}
