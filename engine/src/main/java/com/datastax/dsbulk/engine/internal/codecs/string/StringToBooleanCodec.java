/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
    if (value == null) {
      return null;
    }
    String s = outputs.get(value);
    if (s == null) {
      return Boolean.toString(value);
    }
    return s;
  }
}
