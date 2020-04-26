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
package com.datastax.oss.dsbulk.codecs.jdk.number;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.time.Instant;
import org.junit.jupiter.api.Test;

class NumberToInstantCodecTest {

  private NumberToInstantCodec<Long> codec =
      new NumberToInstantCodec<>(Long.class, MILLISECONDS, EPOCH.atZone(UTC));

  @Test
  void should_convert_when_valid_input() {

    assertThat(codec)
        .convertsFromExternal(123456L)
        .toInternal(Instant.ofEpochMilli(123456L))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(codec)
        .convertsFromInternal(Instant.ofEpochMilli(123456L))
        .toExternal(123456L)
        .convertsFromInternal(null)
        .toExternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
