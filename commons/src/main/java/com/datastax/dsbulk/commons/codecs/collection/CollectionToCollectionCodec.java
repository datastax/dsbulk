/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.collection;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
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
