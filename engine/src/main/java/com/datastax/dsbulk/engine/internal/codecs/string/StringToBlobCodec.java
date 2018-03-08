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
import java.util.List;

public class StringToBlobCodec extends ConvertingCodec<String, ByteBuffer> {

  private final List<String> nullWords;

  public StringToBlobCodec(List<String> nullWords) {
    super(blob(), String.class);
    this.nullWords = nullWords;
  }

  @Override
  public ByteBuffer convertFrom(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
      return null;
    }
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public String convertTo(ByteBuffer value) {
    if (value == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    return Base64.getEncoder().encodeToString(Bytes.getArray(value));
  }
}
