/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.util.Map;

public class StringToBooleanCodec extends ConvertingCodec<String, Boolean> {

  private final Map<String, Boolean> inputs;
  private final Map<Boolean, String> outputs;

  public StringToBooleanCodec(Map<String, Boolean> inputs, Map<Boolean, String> outputs) {
    super(cboolean(), String.class);
    this.inputs = inputs;
    this.outputs = outputs;
  }

  @Override
  public Boolean convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    Boolean b = inputs.get(s.toLowerCase());
    if (b == null) {
      throw new InvalidTypeException("Invalid boolean value: " + s);
    }
    return b;
  }

  @Override
  public String convertTo(Boolean value) {
    return outputs.get(value);
  }
}
