/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string.dse;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;

import com.datastax.dse.driver.api.core.type.search.DateRange;
import java.util.List;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

@Disabled
class StringToDateRangeCodecTest {

  private List<String> nullStrings = newArrayList("NULL");
  private DateRange dateRange = DateRange.parse("[* TO 2014-12-01]");

  StringToDateRangeCodecTest() {}

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
