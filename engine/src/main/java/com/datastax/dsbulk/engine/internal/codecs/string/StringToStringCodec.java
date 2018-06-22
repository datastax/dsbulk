/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;

public class StringToStringCodec extends StringConvertingCodec<String> {

  public StringToStringCodec(TypeCodec<String> innerCodec, List<String> nullStrings) {
    super(innerCodec, nullStrings);
  }

  @Override
  public String externalToInternal(String s) {
    // DAT-297: do not convert empty strings to null so do not use isNullOrEmpty() here
    if (isNull(s)) {
      return null;
    }
    return s;
  }

  @Override
  public String internalToExternal(String value) {
    if (value == null) {
      return nullString();
    }
    return value;
  }
}
