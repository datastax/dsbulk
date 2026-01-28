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
package com.datastax.oss.dsbulk.codecs.api;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;

public class NullAllowingConvertingCodecFactory extends ConvertingCodecFactory {

  private static class NullAllowingConvertingCodec<EXTERNAL, INTERNAL>
      extends ConvertingCodec<EXTERNAL, INTERNAL> {

    private final ConvertingCodec<EXTERNAL, INTERNAL> delegate;

    private NullAllowingConvertingCodec(ConvertingCodec<EXTERNAL, INTERNAL> delegate) {
      super(delegate.getInternalCodec(), delegate.getJavaType());
      this.delegate = delegate;
    }

    @Override
    public EXTERNAL decode(ByteBuffer bytes, @NonNull ProtocolVersion protocolVersion) {
      if (bytes == null || bytes.remaining() == 0) {
        return null;
      }
      return this.delegate.decode(bytes, protocolVersion);
    }

    public INTERNAL externalToInternal(EXTERNAL external) {
      return this.delegate.externalToInternal(external);
    }

    public EXTERNAL internalToExternal(INTERNAL internal) {
      return this.delegate.internalToExternal(internal);
    }
  }

  private final ConvertingCodecFactory delegate;

  public NullAllowingConvertingCodecFactory(ConvertingCodecFactory delegate) {
    this.delegate = delegate;
  }

  @NonNull
  public MutableCodecRegistry getCodecRegistry() {
    return this.delegate.getCodecRegistry();
  }

  @NonNull
  public ConversionContext getContext() {
    return this.delegate.getContext();
  }

  public <EXTERNAL, INTERNAL> ConvertingCodec<EXTERNAL, INTERNAL> createConvertingCodec(
      @NonNull DataType cqlType,
      @NonNull GenericType<EXTERNAL> externalJavaType,
      boolean rootCodec) {

    ConvertingCodec<EXTERNAL, INTERNAL> base =
        this.delegate.createConvertingCodec(cqlType, externalJavaType, rootCodec);
    if (cqlType instanceof ListType || cqlType instanceof SetType || cqlType instanceof MapType) {
      return new NullAllowingConvertingCodec<>(base);
    }
    return base;
  }
}
