/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.Duration;
import java.util.List;

public class StringToDurationCodec extends StringConvertingCodec<Duration> {

  public StringToDurationCodec(List<String> nullStrings) {
    super(duration(), nullStrings);
  }

  @Override
  public Duration externalToInternal(String s) {
    if (isNull(s)) {
      return null;
    }
    return Duration.from(s);
  }

  @Override
  public String internalToExternal(Duration value) {
    if (value == null) {
      return nullString();
    }
    return value.toString();
  }
}
