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
package com.datastax.oss.dsbulk.codecs;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.CodecNotFoundException;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.CopyOnWriteArrayList;

/** A factory for {@link ConvertingCodec}s. */
public class ConvertingCodecFactory {

  private final List<ConvertingCodecProvider> providers = new CopyOnWriteArrayList<>();
  @NonNull private final CodecRegistry codecRegistry;
  private final ConversionContext context;

  public ConvertingCodecFactory() {
    this(DefaultCodecRegistry.DEFAULT, new ConversionContext());
  }

  public ConvertingCodecFactory(@NonNull ConversionContext context) {
    this(DefaultCodecRegistry.DEFAULT, context);
  }

  public ConvertingCodecFactory(
      @NonNull CodecRegistry codecRegistry, @NonNull ConversionContext context) {
    this.codecRegistry = codecRegistry;
    this.context = context;
    ServiceLoader<ConvertingCodecProvider> loader =
        ServiceLoader.load(ConvertingCodecProvider.class);
    for (ConvertingCodecProvider provider : loader) {
      providers.add(provider);
    }
  }

  @NonNull
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }

  @NonNull
  public ConversionContext getContext() {
    return context;
  }

  @NonNull
  public <EXTERNAL, INTERNAL> ConvertingCodec<EXTERNAL, INTERNAL> createConvertingCodec(
      @NonNull DataType cqlType,
      @NonNull GenericType<? extends EXTERNAL> externalJavaType,
      boolean rootCodec) {
    for (ConvertingCodecProvider provider : providers) {
      Optional<ConvertingCodec<?, ?>> maybeCodec =
          provider.maybeProvide(cqlType, externalJavaType, this, rootCodec);
      if (maybeCodec.isPresent()) {
        @SuppressWarnings("unchecked")
        ConvertingCodec<EXTERNAL, INTERNAL> codec =
            (ConvertingCodec<EXTERNAL, INTERNAL>) maybeCodec.get();
        return codec;
      }
    }
    throw new CodecNotFoundException(cqlType, externalJavaType);
  }
}
