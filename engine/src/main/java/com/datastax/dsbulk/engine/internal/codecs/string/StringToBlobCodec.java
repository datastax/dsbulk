/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.Base64;
import java.util.List;

public class StringToBlobCodec extends StringConvertingCodec<ByteBuffer> {

  public StringToBlobCodec(List<String> nullStrings) {
    super(TypeCodecs.BLOB, nullStrings);
  }

  @Override
  public ByteBuffer externalToInternal(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public String internalToExternal(ByteBuffer value) {
    if (value == null) {
      return nullString();
    }
    return Base64.getEncoder().encodeToString(Bytes.getArray(value));
  }
}
