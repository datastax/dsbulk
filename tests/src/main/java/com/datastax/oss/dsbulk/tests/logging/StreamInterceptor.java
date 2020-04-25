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
package com.datastax.oss.dsbulk.tests.logging;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

/** An interceptor for standard out streams (stdout or stderr). */
public interface StreamInterceptor {

  /** @return The contents of the captured stream as a string. */
  default String getStreamAsString() {
    return getStreamAsString(StandardCharsets.UTF_8);
  }

  /**
   * @return The contents of the captured stream as a plain string, ANSI escape codes are stripped
   *     out.
   */
  default String getStreamAsStringPlain() {
    return getStreamAsStringPlain(StandardCharsets.UTF_8);
  }

  /** @return The contents of the captured stream as a string. */
  String getStreamAsString(Charset charset);

  /**
   * @return The contents of the captured stream as a plain string, ANSI escape codes are stripped
   *     out.
   */
  String getStreamAsStringPlain(Charset charset);

  /** @return The contents of the captured stream, line by line. */
  default List<String> getStreamLines() {
    return getStreamLines(StandardCharsets.UTF_8);
  }

  /** @return The contents of the captured stream, line by line. */
  default List<String> getStreamLines(Charset charset) {
    return Arrays.asList(getStreamAsString(charset).split(System.lineSeparator()));
  }

  /**
   * @return The contents of the captured stream, line by line, each line as a plain string, ANSI
   *     escape codes are stripped out.
   */
  default List<String> getStreamLinesPlain() {
    return getStreamLinesPlain(StandardCharsets.UTF_8);
  }

  /**
   * @return The contents of the captured stream, line by line, each line as a plain string, ANSI
   *     escape codes are stripped out.
   */
  default List<String> getStreamLinesPlain(Charset charset) {
    return Arrays.asList(getStreamAsStringPlain(charset).split(System.lineSeparator()));
  }

  /** Clear the intercepted stream. */
  void clear();
}
