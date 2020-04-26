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
package com.datastax.oss.dsbulk.codecs.jdk.collection;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import java.util.Collection;
import java.util.function.Supplier;

public class CollectionToCollectionCodec<
        EXTERNAL extends Collection<Object>, INTERNAL extends Collection<Object>>
    extends ConvertingCodec<EXTERNAL, INTERNAL> {
  private final ConvertingCodec<Object, Object> elementCodec;
  private final Supplier<INTERNAL> collectionCreator;

  public CollectionToCollectionCodec(
      Class<EXTERNAL> javaType,
      TypeCodec<INTERNAL> targetCodec,
      ConvertingCodec<Object, Object> elementCodec,
      Supplier<INTERNAL> collectionCreator) {
    super(targetCodec, javaType);
    this.elementCodec = elementCodec;
    this.collectionCreator = collectionCreator;
  }

  @Override
  public INTERNAL externalToInternal(EXTERNAL external) {
    if (external == null || external.isEmpty()) {
      return null;
    }

    INTERNAL result = collectionCreator.get();
    for (Object item : external) {
      result.add(elementCodec.externalToInternal(item));
    }
    return result;
  }

  @Override
  public EXTERNAL internalToExternal(INTERNAL internal) {
    if (internal == null) {
      return null;
    }
    throw new UnsupportedOperationException(
        "This codec does not support converting from the 'internal' collection to the 'external'");
  }
}
