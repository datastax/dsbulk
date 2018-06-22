/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.format.row;

import static com.datastax.dsbulk.commons.internal.format.row.RowFormatterSymbols.summaryEnd;
import static com.datastax.dsbulk.commons.internal.format.row.RowFormatterSymbols.summaryStart;
import static com.datastax.dsbulk.commons.internal.format.row.RowFormatterSymbols.valuesCount;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A component to format instances of {@link Row}.
 *
 * <p>Its main method is the {@link #format(Row, ProtocolVersion, CodecRegistry) format} method.
 *
 * <p>{@code RowFormatter} provides safeguards to prevent overwhelming your logs with rows having a
 * high number of parameters, or very long parameters.
 *
 * <p>Instances of this class are thread-safe.
 */
public final class RowFormatter {

  /** A special value that conveys the notion of "unlimited". */
  public static final int UNLIMITED = -1;

  public static final int DEFAULT_MAX_VALUE_LENGTH = 50;
  public static final int DEFAULT_MAX_VALUES = 10;

  private static final Logger LOGGER = LoggerFactory.getLogger(RowFormatter.class);

  private final int maxValueLength;
  private final int maxValues;

  /** Creates a row formatter with default settings. */
  public RowFormatter() {
    this(DEFAULT_MAX_VALUE_LENGTH, DEFAULT_MAX_VALUES);
  }

  /**
   * Creates a row formatter with customized settings.
   *
   * @param maxValueLength the maximum length, in numbers of printed characters, allowed for a
   *     single bound value.
   * @param maxValues the maximum number of printed bound values.
   * @throws IllegalArgumentException if {@code maxValueLength} or {@code maxValues} is not &gt; 0,
   *     or {@value UNLIMITED} (unlimited).
   */
  public RowFormatter(int maxValueLength, int maxValues) {
    if (maxValueLength <= 0 && maxValueLength != UNLIMITED)
      throw new IllegalArgumentException(
          "Invalid maxValueLength, should be > 0 or -1 (unlimited), got " + maxValueLength);
    this.maxValueLength = maxValueLength;
    if (maxValues <= 0 && maxValues != UNLIMITED)
      throw new IllegalArgumentException(
          "Invalid maxValues, should be > 0 or -1 (unlimited), got " + maxValues);
    this.maxValues = maxValues;
  }

  /**
   * Formats the given {@link Row row}.
   *
   * @param row The row to format; must not be {@code null}.
   * @param protocolVersion The protocol version in use.
   * @param codecRegistry The codec registry in use.
   * @return The row as a formatted string.
   */
  public String format(Row row, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    try {
      RowWriter out =
          new RowWriter(
              new StringBuilder(), 0, maxValueLength, maxValues, protocolVersion, codecRegistry);
      print(row, out);
      return out.toString();
    } catch (Exception e) {
      try {
        LOGGER.error("Could not format row: " + row, e);
        return row.toString();
      } catch (Exception e1) {
        LOGGER.error("row.toString() failed", e1);
        return "row[?]";
      }
    }
  }

  protected void print(Row row, RowWriter out) {
    printHeader(row, out);
    printValues(row, out);
  }

  protected void printHeader(Row row, RowWriter out) {
    out.appendClassNameAndHashCode(row)
        .append(summaryStart)
        .append(String.format(valuesCount, row.getColumnDefinitions().size()))
        .append(summaryEnd);
  }

  protected void printValues(Row row, RowWriter out) {
    if (row.getColumnDefinitions().size() > 0) {
      ColumnDefinitions metadata = row.getColumnDefinitions();
      if (metadata.size() > 0) {
        for (int i = 0; i < metadata.size(); i++) {
          out.newLine();
          out.indent();
          ColumnDefinition col = metadata.get(i);
          String name = col.getName().asCql(true);
          DataType type = col.getType();
          Object value = null;
          boolean malformed = false;
          try {
            value = row.getObject(i);
          } catch (Exception e) {
            // This means that the row contains a malformed buffer for column i
            malformed = true;
          }
          if (malformed) {
            // instead of failing, print the raw bytes.
            out.appendValue(name, row.getBytesUnsafe(i), DataTypes.BLOB);
            out.append(" (malformed buffer for type ").append(String.valueOf(type)).append(")");
          } else {
            out.appendValue(name, value, type);
          }
          if (out.maxAppendedValuesExceeded()) {
            break;
          }
        }
      }
    }
  }
}
