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
package com.datastax.oss.dsbulk.codecs.api.util;

import com.datastax.oss.driver.api.core.data.ByteUtils;
import java.nio.ByteBuffer;
import java.util.Base64;

public class Base64BinaryFormat implements BinaryFormat {

  public static final Base64BinaryFormat INSTANCE = new Base64BinaryFormat();

  private Base64BinaryFormat() {}

  @Override
  public ByteBuffer parse(String s) {
    if (s == null) {
      return null;
    }
    // DAT-573: consider empty string as empty byte array
    if (s.isEmpty()) {
      return ByteBuffer.allocate(0);
    }
    return ByteBuffer.wrap(Base64.getDecoder().decode(s));
  }

  @Override
  public String format(ByteBuffer bb) {
    if (bb == null) {
      return null;
    }
    if (bb.remaining() == 0) {
      return "";
    }
    return Base64.getEncoder().encodeToString(ByteUtils.getArray(bb));
  }
}
