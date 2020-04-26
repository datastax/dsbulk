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

import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.BOOLEAN_NUMBERS;
import static com.datastax.oss.dsbulk.codecs.jdk.JdkCodecUtils.isNumeric;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecProvider;
import com.datastax.oss.dsbulk.codecs.IdempotentConvertingCodec;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;

public class BooleanConvertingCodecsProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {
    if (externalJavaType.equals(GenericType.BOOLEAN)) {
      if (cqlType == DataTypes.TEXT) {
        return Optional.of(new BooleanToStringCodec());
      }
      if (cqlType == DataTypes.BOOLEAN) {
        return Optional.of(new IdempotentConvertingCodec<>(TypeCodecs.BOOLEAN));
      }
      if (isNumeric(cqlType)) {
        TypeCodec<Number> typeCodec = codecFactory.getCodecRegistry().codecFor(cqlType);
        return Optional.of(
            new BooleanToNumberCodec<>(
                typeCodec, codecFactory.getContext().getAttribute(BOOLEAN_NUMBERS)));
      }
    }
    return Optional.empty();
  }
}
