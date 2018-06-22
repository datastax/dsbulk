/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.oss.driver.api.core.data.CqlDuration;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.util.List;

public class StringToDurationCodec extends StringConvertingCodec<CqlDuration> {

  public StringToDurationCodec(List<String> nullStrings) {
    super(TypeCodecs.DURATION, nullStrings);
  }

  @Override
  public CqlDuration externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CqlDuration.from(s);
  }

  @Override
  public String internalToExternal(CqlDuration value) {
    if (value == null) {
      return nullString();
    }
    return value.toString();
  }
}
