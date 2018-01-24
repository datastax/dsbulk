/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;

public class StringToStringCodec extends ConvertingCodec<String, String> {

  public StringToStringCodec(TypeCodec<String> innerCodec) {
    super(innerCodec, String.class);
  }

  @Override
  public String convertFrom(String s) {
    return s;
  }

  @Override
  public String convertTo(String value) {
    return value;
  }
}
