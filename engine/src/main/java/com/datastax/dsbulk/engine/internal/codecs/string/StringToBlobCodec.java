/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.utils.Bytes;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.nio.ByteBuffer;
import java.util.Base64;

public class StringToBlobCodec extends ConvertingCodec<String, ByteBuffer> {

  public static final StringToBlobCodec INSTANCE = new StringToBlobCodec();

  private StringToBlobCodec() {
    super(blob(), String.class);
  }

  @Override
  public ByteBuffer convertFrom(String s) {
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public String convertTo(ByteBuffer value) {
    if (value == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(Bytes.getArray(value));
  }
}
