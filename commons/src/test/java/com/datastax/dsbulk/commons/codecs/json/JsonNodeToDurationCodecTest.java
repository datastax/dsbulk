/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.json;

import static com.datastax.dsbulk.commons.codecs.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import org.junit.jupiter.api.Test;

class JsonNodeToDurationCodecTest {

  private final long nanosPerMinute = 60 * 1000L * 1000L * 1000L;

  private final CqlDuration duration = CqlDuration.newInstance(15, 0, 130 * nanosPerMinute);

  private final JsonNodeToDurationCodec codec = new JsonNodeToDurationCodec(newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("1y3mo2h10m")) // standard pattern
        .toInternal(duration)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("P1Y3MT2H10M")) // ISO 8601 pattern
        .toInternal(duration)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode("P0001-03-00T02:10:00")) // alternative ISO 8601 pattern
        .toInternal(duration)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(duration)
        .toExternal(JSON_NODE_FACTORY.textNode("1y3mo2h10m"))
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal(
            JSON_NODE_FACTORY.textNode("1Y3M4D")) // The minutes should be after days
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid duration"));
  }
}
