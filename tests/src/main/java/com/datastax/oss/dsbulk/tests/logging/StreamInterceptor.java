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

public interface StreamInterceptor {

  default String getStreamAsString() {
    return getStreamAsString(StandardCharsets.UTF_8);
  }

  String getStreamAsString(Charset charset);

  default List<String> getStreamLines() {
    return getStreamLines(StandardCharsets.UTF_8);
  }

  default List<String> getStreamLines(Charset charset) {
    return Arrays.asList(getStreamAsString(charset).split(System.lineSeparator()));
  }

  void clear();
}
