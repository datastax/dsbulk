/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.nio.ByteBuffer;
import java.util.Base64;

public class StringToBlobCodec extends ConvertingCodec<String, ByteBuffer> {

  public static final StringToBlobCodec INSTANCE = new StringToBlobCodec();

  private StringToBlobCodec() {
    super(blob(), String.class);
  }

  @Override
  public ByteBuffer convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    try {
      return Bytes.fromHexString(s);
    } catch (IllegalArgumentException e) {
      try {
        return ByteBuffer.wrap(Base64.getDecoder().decode(s));
      } catch (IllegalArgumentException e1) {
        e1.addSuppressed(e);
        throw new InvalidTypeException("Invalid binary string: " + s, e1);
      }
    }
  }

  @Override
  public String convertTo(ByteBuffer value) {
    if (value == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(Bytes.getArray(value));
  }
}
