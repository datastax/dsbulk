/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string.dse;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.text.ParseException;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToDateRangeCodecTest {

  private List<String> nullStrings = Lists.newArrayList("NULL");
  private DateRange dateRange;

  StringToDateRangeCodecTest() {
    try {
      dateRange = DateRange.parse("[* TO 2014-12-01]");
    } catch (ParseException e) {
      // swallow; can't happen.
    }
  }

  @Test
  void should_convert_from_valid_external() {

    StringToDateRangeCodec codec = new StringToDateRangeCodec(nullStrings);
    assertThat(codec)
        .convertsFromExternal("[* TO 2014-12-01]")
        .toInternal(dateRange)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("")
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
