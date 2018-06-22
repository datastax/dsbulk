/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.json.dse;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.JSON_NODE_FACTORY;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dsbulk.engine.internal.DateRange;
import java.util.List;
import org.junit.jupiter.api.Test;

class JsonNodeToDateRangeCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private DateRange dateRange = DateRange.parse("[* TO 2014-12-01]");

  JsonNodeToDateRangeCodecTest() {}

  @Test
  void should_convert_from_valid_external() {

    JsonNodeToDateRangeCodec codec = new JsonNodeToDateRangeCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("[* TO 2014-12-01]"))
        .toInternal(dateRange)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    JsonNodeToDateRangeCodec codec = new JsonNodeToDateRangeCodec(nullStrings);
    assertThat(codec)
        .convertsFromInternal(dateRange)
        .toExternal(JSON_NODE_FACTORY.textNode("[* TO 2014-12-01]"));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    JsonNodeToDateRangeCodec codec = new JsonNodeToDateRangeCodec(nullStrings);
    assertThat(codec)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid date range literal"));
  }
}
