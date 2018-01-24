/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import java.io.PrintWriter;
import java.net.URI;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** */
public class LogUtils {

  private static final Pattern NEW_LINE = Pattern.compile("\\R");

  private static final int MAX_SOURCE_LENGTH = 500;

  public static void appendStatementInfo(UnmappableStatement statement, PrintWriter writer) {
    URI location = statement.getLocation();
    Record record = statement.getSource();
    writer.println("Location: " + location);
    writer.println("Resource: " + record.getResource());
    writer.println("Position: " + record.getPosition());
    writer.println("Source  : " + formatSource(record));
  }

  public static void appendRecordInfo(Record record, PrintWriter writer) {
    appendRecordInfo(record, writer::println);
  }

  public static void appendRecordInfo(Record record, Consumer<Object> println) {
    println.accept("Location: " + record.getLocation());
    println.accept("Resource: " + record.getResource());
    println.accept("Position: " + record.getPosition());
    println.accept("Source  : " + formatSource(record));
  }

  private static String formatSource(Record record) {
    if (record == null) return "<NULL>";
    String source = record.getSource().toString();
    return formatSingleLine(source);
  }

  public static String formatSingleLine(String string) {
    if (string == null) return "<NULL>";
    if (string.length() > MAX_SOURCE_LENGTH)
      string = string.substring(0, MAX_SOURCE_LENGTH) + "...";
    Matcher matcher = NEW_LINE.matcher(string);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String replacement =
          "\\\\u" + Integer.toHexString(matcher.group().charAt(0) | 0x10000).substring(1);
      matcher.appendReplacement(sb, replacement);
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  public static void printAndMaybeAddNewLine(String string, PrintWriter writer) {
    if (string == null || string.isEmpty()) {
      writer.println();
    } else {
      writer.print(string);
      char last = string.charAt(string.length() - 1);
      if (last != '\n' && last != '\r') {
        writer.println();
      }
    }
  }
}
