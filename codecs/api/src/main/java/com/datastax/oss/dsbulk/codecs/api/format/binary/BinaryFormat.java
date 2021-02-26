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
package com.datastax.oss.dsbulk.codecs.api.format.binary;

import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;

/** An utility for parsing and formatting binary data. */
public interface BinaryFormat {

  /**
   * Parses the given string as a {@link ByteBuffer}.
   *
   * @param s the string to parse, may be {@code null}.
   * @return a {@link ByteBuffer} or {@code null} if the string was {@code null}.
   */
  @Nullable
  ByteBuffer parse(@Nullable String s);

  /**
   * Formats the given {@link ByteBuffer} as a string.
   *
   * @param bytes the value to format, may be {@code null}.
   * @return the formatted value or {@code null} if the value was {@code null}.
   */
  @Nullable
  String format(@Nullable ByteBuffer bytes);
}
