/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log;

import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.internal.schema.UnmappableStatement;
import java.io.PrintWriter;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** */
public class LogUtils {

  private static final Pattern NEW_LINE = Pattern.compile("\\R");

  private static final int MAX_SOURCE_LENGTH = 500;

  public static void appendStatementInfo(UnmappableStatement statement, PrintWriter writer) {
    writer.println("Location: " + statement.getLocation());
    writer.println("Source  : " + formatSource(statement.getSource()));
  }

  public static void appendRecordInfo(Record record, PrintWriter writer) {
    appendRecordInfo(record, writer::println);
  }

  public static void appendRecordInfo(Record record, Consumer<Object> println) {
    println.accept("Location: " + record.getLocation());
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
