/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;

public class BooleanToStringCodec extends ConvertingCodec<Boolean, String> {

  public BooleanToStringCodec() {
    super(TypeCodecs.TEXT, Boolean.class);
  }

  @Override
  public Boolean internalToExternal(String value) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from string to boolean");
  }

  @Override
  public String externalToInternal(Boolean value) {
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
