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
package com.datastax.oss.dsbulk.workflow.commons.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.regex.Pattern;
import javax.management.ObjectName;

public class JMXUtils {

  private static final Pattern MBEAN_VALID_CHARS_PATTERN = Pattern.compile("[a-zA-Z0-9\\-_]+");

  /**
   * Returns the given string quoted with {@link ObjectName#quote(String)} if it contains illegal
   * characters; otherwise, returns the original string.
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
