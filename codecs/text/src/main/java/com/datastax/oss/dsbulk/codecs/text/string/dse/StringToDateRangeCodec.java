/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string.dse;

import com.datastax.dse.driver.api.core.data.time.DateRange;
import com.datastax.dse.driver.internal.core.type.codec.time.DateRangeCodec;
import com.datastax.oss.dsbulk.codecs.text.string.StringConvertingCodec;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
import java.util.List;

public class StringToDateRangeCodec extends StringConvertingCodec<DateRange> {

  public StringToDateRangeCodec(List<String> nullStrings) {
    super(new DateRangeCodec(), nullStrings);
  }

  @Override
  public DateRange externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parseDateRange(s);
  }

  @Override
  public String internalToExternal(DateRange value) {
    if (value == null) {
      return nullString();
    }
    return value.toString();
  }
}
