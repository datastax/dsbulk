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
package com.datastax.oss.dsbulk.commons.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Collections;
import java.util.regex.Pattern;
import javax.management.ObjectName;

public class StringUtils {

  private static final Pattern MBEAN_VALID_CHARS_PATTERN = Pattern.compile("[a-zA-Z0-9\\-_]+");

  /**
   * If the given string is surrounded by double-quotes, return it intact, otherwise trim it and
   * surround it with double-quotes.
   *
   * @param value The string to check.
   * @return A string surrounded by double-quotes.
   */
  public static String ensureQuoted(String value) {
    value = value.trim();
    if (value.startsWith("\"") && value.endsWith("\"")) {
      return value;
    }
    return "\"" + value + "\"";
  }

  /**
   * If the given string is surrounded by square brackets, return it intact, otherwise trim it and
   * surround it with square brackets.
   *
   * @param value The string to check.
   * @return A string surrounded by square brackets.
   */
  public static String ensureBrackets(String value) {
    value = value.trim();
    if (value.startsWith("[") && value.endsWith("]")) {
      return value;
    }
    return "[" + value + "]";
  }

  /**
   * If the given string is surrounded by curly braces, return it intact, otherwise trim it and
   * surround it with curly braces.
   *
   * @param value The string to check.
   * @return A string surrounded by curly braces.
   */
  public static String ensureBraces(String value) {
    value = value.trim();
    if (value.startsWith("{") && value.endsWith("}")) {
      return value;
    }
    return "{" + value + "}";
  }

  /**
   * Upper-cases the first letter of the given string
   *
   * @param s String to convert.
   * @return The string with its first letter in upper case.
   */
  public static String ucfirst(String s) {
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  /**
   * Returns a new string consisting of n copies of the given string.
   *
   * @param s String to copy.
   * @param count Number of times to copy it.
   * @return a new string consisting of n copies of the given string.
   */
  public static String nCopies(String s, int count) {
    return String.join("", Collections.nCopies(count, s));
  }

  /**
   * Replaces tokens that are meaningful in html with their entity representations.
   *
   * @param s String to escape.
   * @return the escaped string.
   */
  public static String htmlEscape(String s) {
    return s.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  }

  /**
   * Left pad a string with spaces to a size of {@code size}.
   *
   * @param s String to pad.
   * @param size the size.
   * @return the padded string.
   */
  public static String leftPad(String s, int size) {
    int repeat = size - s.length();
    if (repeat <= 0) {
      return s;
    }
    char[] buf = new char[repeat];
    for (int i = 0; i < repeat; i++) {
      buf[i] = ' ';
    }
    return new String(buf).concat(s);
  }

  /**
   * Returns the given string quoted with {@link javax.management.ObjectName#quote(String)} if it
   * contains illegal characters; otherwise, returns the original string.
   *
   * <p>The <a
   * href="https://www.oracle.com/technetwork/java/javase/tech/best-practices-jsp-136021.html#mozTocId434075">Object
   * Name Syntax</a> does not define which characters are legal or not in a property value;
   * therefore this method adopts a conservative approach and quotes all values that do not match
   * the regex {@code [a-zA-Z0-9_]+}.
   *
   * @param value The value to quote if necessary.
   * @return The value quoted if necessary, or the original value if quoting isn't required.
   */
  @NonNull
  public static String quoteJMXIfNecessary(@NonNull String value) {
    if (MBEAN_VALID_CHARS_PATTERN.matcher(value).matches()) {
      return value;
    }
    return ObjectName.quote(value);
  }
}
