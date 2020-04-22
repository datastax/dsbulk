/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.commons.utils;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

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

  public static String formatElapsed(long seconds) {
    long hr = SECONDS.toHours(seconds);
    long min = SECONDS.toMinutes(seconds - HOURS.toSeconds(hr));
    long sec = seconds - HOURS.toSeconds(hr) - MINUTES.toSeconds(min);
    if (hr > 0) {
      return String.format("%d hours, %d minutes and %d seconds", hr, min, sec);
    } else if (min > 0) {
      return String.format("%d minutes and %d seconds", min, sec);
    } else {
      return String.format("%d seconds", sec);
    }
  }
}
