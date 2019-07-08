/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.regex.Pattern;
import javax.management.ObjectName;
import org.jetbrains.annotations.NotNull;

public class StringUtils {

  /**
   * Return {@code true} if the given string is surrounded by single quotes, and {@code false}
   * otherwise.
   *
   * @param value The string to inspect.
   * @return {@code true} if the given string is surrounded by single quotes, and {@code false}
   *     otherwise.
   */
  public static boolean isQuoted(String value) {
    return isQuoted(value, '\'');
  }

  /**
   * Quote the given string; single quotes are escaped. If the given string is null, this method
   * returns a quoted empty string ({@code ''}).
   *
   * @param value The value to quote.
   * @return The quoted string.
   */
  public static String quote(String value) {
    return quote(value, '\'');
  }

  /**
   * Unquote the given string if it is quoted; single quotes are unescaped. If the given string is
   * not quoted, it is returned without any modification.
   *
   * @param value The string to unquote.
   * @return The unquoted string.
   */
  public static String unquote(String value) {
    return unquote(value, '\'');
  }

  /**
   * Return {@code true} if the given string is surrounded by double quotes, and {@code false}
   * otherwise.
   *
   * @param value The string to inspect.
   * @return {@code true} if the given string is surrounded by double quotes, and {@code false}
   *     otherwise.
   */
  public static boolean isDoubleQuoted(String value) {
    return isQuoted(value, '\"');
  }

  /**
   * Double quote the given string; double quotes are escaped. If the given string is null, this
   * method returns a quoted empty string ({@code ""}).
   *
   * @param value The value to double quote.
   * @return The double quoted string.
   */
  public static String doubleQuote(String value) {
    return quote(value, '"');
  }

  /**
   * Unquote the given string if it is double quoted; double quotes are unescaped. If the given
   * string is not double quoted, it is returned without any modification.
   *
   * @param value The string to un-double quote.
   * @return The un-double quoted string.
   */
  public static String unDoubleQuote(String value) {
    return unquote(value, '"');
  }

  /** Whether a string needs double quotes to be a valid CQL identifier. */
  public static boolean needsDoubleQuotes(String s) {
    // this method should only be called for C*-provided identifiers,
    // so we expect it to be non-null and non-empty.
    assert s != null && !s.isEmpty();
    char c = s.charAt(0);
    if (!(c >= 97 && c <= 122)) // a-z
    return true;
    for (int i = 1; i < s.length(); i++) {
      c = s.charAt(i);
      if (!((c >= 48 && c <= 57) // 0-9
          || (c == 95) // _
          || (c >= 97 && c <= 122) // a-z
      )) {
        return true;
      }
    }
    return isReservedCqlKeyword(s);
  }

  /**
   * Return {@code true} if the given string is surrounded by the quote character given, and {@code
   * false} otherwise.
   *
   * @param value The string to inspect.
   * @return {@code true} if the given string is surrounded by the quote character, and {@code
   *     false} otherwise.
   */
  private static boolean isQuoted(String value, char quoteChar) {
    return value != null
        && value.length() > 1
        && value.charAt(0) == quoteChar
        && value.charAt(value.length() - 1) == quoteChar;
  }

  /**
   * @param quoteChar " or '
   * @return A quoted empty string.
   */
  private static String emptyQuoted(char quoteChar) {
    // don't handle non quote characters, this is done so that these are interned and don't create
    // repeated empty quoted strings.
    assert quoteChar == '"' || quoteChar == '\'';
    if (quoteChar == '"') return "\"\"";
    else return "''";
  }

  /**
   * Quotes text and escapes any existing quotes in the text. {@code String.replace()} is a bit too
   * inefficient (see JAVA-67, JAVA-1262).
   *
   * @param text The text.
   * @param quoteChar The character to use as a quote.
   * @return The text with surrounded in quotes with all existing quotes escaped with (i.e. '
   *     becomes '')
   */
  private static String quote(String text, char quoteChar) {
    if (text == null || text.isEmpty()) return emptyQuoted(quoteChar);

    int nbMatch = 0;
    int start = -1;
    do {
      start = text.indexOf(quoteChar, start + 1);
      if (start != -1) ++nbMatch;
    } while (start != -1);

    // no quotes found that need to be escaped, simply surround in quotes and return.
    if (nbMatch == 0) return quoteChar + text + quoteChar;

    // 2 for beginning and end quotes.
    // length for original text
    // nbMatch for escape characters to add to quotes to be escaped.
    int newLength = 2 + text.length() + nbMatch;
    char[] result = new char[newLength];
    result[0] = quoteChar;
    result[newLength - 1] = quoteChar;
    int newIdx = 1;
    for (int i = 0; i < text.length(); i++) {
      char c = text.charAt(i);
      if (c == quoteChar) {
        // escape quote with another occurrence.
        result[newIdx++] = c;
        result[newIdx++] = c;
      } else {
        result[newIdx++] = c;
      }
    }
    return new String(result);
  }

  /**
   * Unquotes text and unescapes non surrounding quotes. {@code String.replace()} is a bit too
   * inefficient (see JAVA-67, JAVA-1262).
   *
   * @param text The text
   * @param quoteChar The character to use as a quote.
   * @return The text with surrounding quotes removed and non surrounding quotes unescaped (i.e. ''
   *     becomes ')
   */
  private static String unquote(String text, char quoteChar) {
    if (!isQuoted(text, quoteChar)) return text;

    if (text.length() == 2) return "";

    String search = emptyQuoted(quoteChar);
    int nbMatch = 0;
    int start = -1;
    do {
      start = text.indexOf(search, start + 2);
      // ignore the second to last character occurrence, as the last character is a quote.
      if (start != -1 && start != text.length() - 2) ++nbMatch;
    } while (start != -1);

    // no escaped quotes found, simply remove surrounding quotes and return.
    if (nbMatch == 0) return text.substring(1, text.length() - 1);

    // length of the new string will be its current length - the number of occurrences.
    int newLength = text.length() - nbMatch - 2;
    char[] result = new char[newLength];
    int newIdx = 0;
    // track whenever a quoteChar is encountered and the previous character is not a quoteChar.
    boolean firstFound = false;
    for (int i = 1; i < text.length() - 1; i++) {
      char c = text.charAt(i);
      if (c == quoteChar) {
        if (firstFound) {
          // The previous character was a quoteChar, don't add this to result, this action in
          // effect removes consecutive quotes.
          firstFound = false;
        } else {
          // found a quoteChar and the previous character was not a quoteChar, include in result.
          firstFound = true;
          result[newIdx++] = c;
        }
      } else {
        // non quoteChar encountered, include in result.
        result[newIdx++] = c;
        firstFound = false;
      }
    }
    return new String(result);
  }

  private static boolean isReservedCqlKeyword(String id) {
    return id != null && RESERVED_KEYWORDS.contains(id.toLowerCase());
  }

  private static final ImmutableSet<String> RESERVED_KEYWORDS =
      ImmutableSet.of(
          // See https://github.com/apache/cassandra/blob/trunk/doc/cql3/CQL.textile#appendixA
          "add",
          "allow",
          "alter",
          "and",
          "any",
          "apply",
          "asc",
          "authorize",
          "batch",
          "begin",
          "by",
          "columnfamily",
          "create",
          "delete",
          "desc",
          "drop",
          "each_quorum",
          "from",
          "grant",
          "in",
          "index",
          "inet",
          "infinity",
          "insert",
          "into",
          "keyspace",
          "keyspaces",
          "limit",
          "local_one",
          "local_quorum",
          "modify",
          "nan",
          "norecursive",
          "of",
          "on",
          "one",
          "order",
          "password",
          "primary",
          "quorum",
          "rename",
          "revoke",
          "schema",
          "select",
          "set",
          "table",
          "to",
          "token",
          "three",
          "truncate",
          "two",
          "unlogged",
          "update",
          "use",
          "using",
          "where",
          "with");

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
  @NotNull
  public static String quoteJMXIfNecessary(@NotNull String value) {
    Pattern pattern = Pattern.compile("[a-zA-Z0-9\\-_]+");
    if (pattern.matcher(value).matches()) {
      return value;
    }
    return ObjectName.quote(value);
  }
}
