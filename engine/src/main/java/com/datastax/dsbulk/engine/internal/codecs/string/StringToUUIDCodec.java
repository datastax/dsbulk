/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.util.UUID;

public class StringToUUIDCodec extends ConvertingCodec<String, UUID> {

  public StringToUUIDCodec(TypeCodec<UUID> targetCodec) {
    super(targetCodec, String.class);
  }

  @Override
  public UUID convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return UUID.fromString(s);
    } catch (IllegalArgumentException e) {
      throw new InvalidTypeException("Invalid UUID string: " + s);
    }
  }

  @Override
  public String convertTo(UUID value) {
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
