/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.extras.codecs.jdk8.LocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class StringToLocalDateCodec extends StringToTemporalCodec<LocalDate> {

  public StringToLocalDateCodec(DateTimeFormatter parser, List<String> nullStrings) {
    super(LocalDateCodec.instance, parser, nullStrings);
  }

  @Override
  public LocalDate externalToInternal(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toLocalDate(temporal, temporalFormat.getZone());
  }
}
