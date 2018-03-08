/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.extras.codecs.jdk8.LocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public class StringToLocalTimeCodec extends StringToTemporalCodec<LocalTime> {

  public StringToLocalTimeCodec(DateTimeFormatter parser, List<String> nullWords) {
    super(LocalTimeCodec.instance, parser, nullWords);
  }

  @Override
  public LocalTime externalToInternal(String s) {
    TemporalAccessor temporal = parseTemporalAccessor(s);
    if (temporal == null) {
      return null;
    }
    return CodecUtils.toLocalTime(temporal, temporalFormat.getZone());
  }
}
