/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.dsbulk.codecs.util.CodecUtils;
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
    // DAT-573: consider empty strings as empty byte arrays
    if (isNull(s)) {
      return null;
    }
    return CodecUtils.parseByteBuffer(s);
  }

  @Override
  public String internalToExternal(ByteBuffer value) {
    if (value == null) {
      return nullString();
    }
    if (!value.hasRemaining()) {
      return "";
    }
    return Base64.getEncoder().encodeToString(Bytes.getArray(value));
  }
}
