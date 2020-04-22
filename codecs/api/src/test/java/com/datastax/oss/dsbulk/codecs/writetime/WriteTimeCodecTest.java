/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.writetime;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
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
