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
package com.datastax.oss.dsbulk.codecs.api.writetime;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.Test;

class WriteTimeCodecTest {

  @Test
  void should_convert_to_timestamp_micros() {
    @SuppressWarnings("unchecked")
    ConvertingCodec<String, Instant> innerCodec = mock(ConvertingCodec.class);
    String text = "2017-11-30T14:46:56+01:00";
    Instant instant = ZonedDateTime.parse(text).toInstant();
    when(innerCodec.externalToInternal(eq(text))).thenReturn(instant);
    assertThat(new WriteTimeCodec<>(innerCodec).externalToInternal(text))
        .isEqualTo(MILLISECONDS.toMicros(instant.toEpochMilli()));
  }
}
