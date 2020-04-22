/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;

public class StringToUnknownTypeCodec<T> extends StringConvertingCodec<T> {

  public StringToUnknownTypeCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, nullStrings);
  }

  @Override
  public T externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return getInternalCodec().parse(s);
  }

  @Override
  public String internalToExternal(T o) {
    if (o == null) {
      return nullString();
    }
    String s = getInternalCodec().format(o);
    // most codecs usually format null/empty values using the CQL keyword "NULL"
    if (s.equalsIgnoreCase("NULL")) {
      return nullString();
    }
    return s;
  }
}
