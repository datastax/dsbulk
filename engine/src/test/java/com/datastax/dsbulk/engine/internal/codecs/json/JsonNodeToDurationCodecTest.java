/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.Duration;
import org.junit.jupiter.api.Test;

class JsonNodeToDurationCodecTest {

  private final long nanosPerMinute = 60 * 1000L * 1000L * 1000L;

  private final Duration duration = Duration.newInstance(15, 0, 130 * nanosPerMinute);

  private final JsonNodeToDurationCodec codec = JsonNodeToDurationCodec.INSTANCE;

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom(JSON_NODE_FACTORY.textNode("1y3mo2h10m")) // standard pattern
        .to(duration)
        .convertsFrom(JSON_NODE_FACTORY.textNode("P1Y3MT2H10M")) // ISO 8601 pattern
        .to(duration)
        .convertsFrom(
            JSON_NODE_FACTORY.textNode("P0001-03-00T02:10:00")) // alternative ISO 8601 pattern
        .to(duration)
        .convertsFrom(JSON_NODE_FACTORY.textNode(""))
        .to(null)
        .convertsFrom(JSON_NODE_FACTORY.nullNode())
        .to(null)
        .convertsFrom(null)
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(duration)
        .from(JSON_NODE_FACTORY.textNode("1y3mo2h10m"))
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec)
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("1Y3M4D")) // The minutes should be after days
        .cannotConvertFrom(JSON_NODE_FACTORY.textNode("not a valid duration"));
  }
}
