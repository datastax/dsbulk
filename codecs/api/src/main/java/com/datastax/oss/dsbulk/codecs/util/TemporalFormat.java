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
package com.datastax.oss.dsbulk.codecs.util;

import java.time.DateTimeException;
import java.time.temporal.TemporalAccessor;

/**
 * A small wrapper around {@link java.time.format.DateTimeFormatter} that allows to use different
 * formats when parsing and formatting.
 */
public interface TemporalFormat {

  /**
   * Parses the given string as a temporal.
   *
   * @param text the string to parse, may be {@code null}.
   * @return a {@link TemporalAccessor} or {@code null} if the string was {@code null} or empty.
   * @throws DateTimeException if the string cannot be parsed.
   */
  TemporalAccessor parse(String text) throws DateTimeException;

  /**
   * Formats the given temporal.
   *
   * @param temporal the value to format.
   * @return the formatted value or {@code null} if the value was {@code null}.
   * @throws DateTimeException if the value cannot be formatted.
   */
  String format(TemporalAccessor temporal) throws DateTimeException;
}
