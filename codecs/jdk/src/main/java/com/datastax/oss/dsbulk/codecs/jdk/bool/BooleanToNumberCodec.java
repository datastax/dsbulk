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
package com.datastax.oss.dsbulk.codecs.jdk.bool;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.util.CodecUtils;
import java.math.BigDecimal;
import java.util.List;
import java.util.stream.Collectors;

public class BooleanToNumberCodec<INTERNAL extends Number>
    extends ConvertingCodec<Boolean, INTERNAL> {

  private final List<INTERNAL> booleanNumbers;

  @SuppressWarnings("unchecked")
  public BooleanToNumberCodec(TypeCodec<INTERNAL> targetCodec, List<BigDecimal> booleanNumbers) {
    super(targetCodec, Boolean.class);
    this.booleanNumbers =
        booleanNumbers.stream()
            .map(
                n ->
                    CodecUtils.convertNumber(
                        n, (Class<INTERNAL>) targetCodec.getJavaType().getRawType()))
            .collect(Collectors.toList());
  }

  @Override
  public INTERNAL externalToInternal(Boolean value) {
    if (value == null) {
      return null;
    }
    return booleanNumbers.get(value ? 0 : 1);
  }

  @Override
  public Boolean internalToExternal(INTERNAL value) {
    if (value == null) {
      return null;
    }
    int i = booleanNumbers.indexOf(value);
    if (i == -1) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid boolean number %s, accepted values are %s (true) and %s (false)",
              value, booleanNumbers.get(0), booleanNumbers.get(1)));
    }
    return i == 0;
  }
}
