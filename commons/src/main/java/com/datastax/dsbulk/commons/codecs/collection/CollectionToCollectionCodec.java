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
import java.util.ArrayList;
import java.util.Collection;
import java.util.function.Supplier;

public class CollectionToCollectionCodec<
        E, I, EXTERNAL extends Collection<E>, INTERNAL extends Collection<I>>
    extends ConvertingCodec<EXTERNAL, INTERNAL> {
  private TypeCodec<?> elementCodec;
  private Supplier<INTERNAL> collectionCreator;

  public CollectionToCollectionCodec(
      Class<EXTERNAL> javaType,
      TypeCodec<INTERNAL> targetCodec,
      TypeCodec<?> elementCodec,
      Supplier<INTERNAL> collectionCreator) {
    super(targetCodec, javaType);
    this.elementCodec = elementCodec;
    this.collectionCreator = collectionCreator;
  }

  @SuppressWarnings("unchecked")
  @Override
  public INTERNAL externalToInternal(EXTERNAL external) {
    if (external == null || external.isEmpty()) {
      return null;
    }

    INTERNAL result = collectionCreator.get();
    for (E item : external) {
      if (!(elementCodec instanceof ConvertingCodec)) {
        result.add((I) item);
      } else {
        result.add((I) ((ConvertingCodec) elementCodec).externalToInternal(item));
      }
    }
    return result;
  }

  @SuppressWarnings("unchecked")
  @Override
  public EXTERNAL internalToExternal(INTERNAL internal) {
    // TODO
    return (EXTERNAL) new ArrayList(internal);
  }
}
