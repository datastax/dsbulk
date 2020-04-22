/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
