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
package com.datastax.oss.dsbulk.codecs.api.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BinaryFormatTest {

  private static final ByteBuffer EMPTY = ByteBuffer.wrap(new byte[] {});
  private static final ByteBuffer DATA = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5, 6});

  @ParameterizedTest
  @MethodSource
  void should_parse(BinaryFormat format, String input, ByteBuffer expected) {
    assertThat(format.parse(input)).isEqualTo(expected);
  }

  static Stream<Arguments> should_parse() {
    return Stream.of(
        Arguments.of(HexBinaryFormat.INSTANCE, null, null),
        Arguments.of(HexBinaryFormat.INSTANCE, "", EMPTY),
        Arguments.of(HexBinaryFormat.INSTANCE, "0x", EMPTY),
        Arguments.of(HexBinaryFormat.INSTANCE, "0x010203040506", DATA),
        Arguments.of(Base64BinaryFormat.INSTANCE, null, null),
        Arguments.of(Base64BinaryFormat.INSTANCE, "", EMPTY),
        Arguments.of(Base64BinaryFormat.INSTANCE, "AQIDBAUG", DATA));
  }

  @ParameterizedTest
  @MethodSource
  void should_format(BinaryFormat format, ByteBuffer input, String expected) {
    assertThat(format.format(input)).isEqualTo(expected);
  }

  static Stream<Arguments> should_format() {
    return Stream.of(
        Arguments.of(HexBinaryFormat.INSTANCE, null, null),
        Arguments.of(HexBinaryFormat.INSTANCE, EMPTY, "0x"),
        Arguments.of(HexBinaryFormat.INSTANCE, DATA, "0x010203040506"),
        Arguments.of(Base64BinaryFormat.INSTANCE, null, null),
        Arguments.of(Base64BinaryFormat.INSTANCE, EMPTY, ""),
        Arguments.of(Base64BinaryFormat.INSTANCE, DATA, "AQIDBAUG"));
  }
}
