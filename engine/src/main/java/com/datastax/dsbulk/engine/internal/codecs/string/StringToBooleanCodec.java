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
import java.util.List;
import java.util.Map;

public class StringToBooleanCodec extends ConvertingCodec<String, Boolean> {

  private final Map<String, Boolean> inputs;
  private final Map<Boolean, String> outputs;
  private final List<String> nullWords;

  public StringToBooleanCodec(
      Map<String, Boolean> inputs, Map<Boolean, String> outputs, List<String> nullWords) {
    super(cboolean(), String.class);
    this.inputs = inputs;
    this.outputs = outputs;
    this.nullWords = nullWords;
  }

  @Override
  public Boolean externalToInternal(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
      return null;
    }
    Boolean b = inputs.get(s.toLowerCase());
    if (b == null) {
      throw new InvalidTypeException("Invalid boolean value: " + s);
    }
    return b;
  }

  @Override
  public String internalToExternal(Boolean value) {
    if (value == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    String s = outputs.get(value);
    if (s == null) {
      return Boolean.toString(value);
    }
    return s;
  }
}
