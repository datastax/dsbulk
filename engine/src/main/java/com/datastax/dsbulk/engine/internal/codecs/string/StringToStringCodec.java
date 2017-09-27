/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
