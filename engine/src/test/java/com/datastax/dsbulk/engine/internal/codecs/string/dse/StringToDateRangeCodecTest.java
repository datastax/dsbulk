/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string.dse;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.driver.dse.search.DateRange;
import java.text.ParseException;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToDateRangeCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private DateRange dateRange = DateRange.parse("[* TO 2014-12-01]");

  StringToDateRangeCodecTest() throws ParseException {}

  @Test
  void should_convert_from_valid_external() {

    StringToDateRangeCodec codec = new StringToDateRangeCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal("[* TO 2014-12-01]")
        .toInternal(dateRange)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToDateRangeCodec codec = new StringToDateRangeCodec(nullStrings);
    assertThat(codec).convertsFromInternal(dateRange).toExternal("[* TO 2014-12-01]");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToDateRangeCodec codec = new StringToDateRangeCodec(nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid date range literal");
  }
}
