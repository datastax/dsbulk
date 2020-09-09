/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.log;

import com.datastax.oss.dsbulk.connectors.api.Record;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.PrintWriter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogManagerUtils {

  private static final Pattern NEW_LINE = Pattern.compile("\\R");

  private static final int MAX_SOURCE_LENGTH = 500;

  public static String formatSource(@NonNull Record record) {
    if (record.getSource() == null) {
      return "<NULL>";
    }
    String source = record.getSource().toString();
    return formatSingleLine(source);
  }

  public static String formatSingleLine(String string) {
    if (string == null) {
      return "<NULL>";
    }
    if (string.length() > MAX_SOURCE_LENGTH) {
      string = string.substring(0, MAX_SOURCE_LENGTH) + "...";
    }
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
