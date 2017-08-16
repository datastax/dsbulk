/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

public class StringToSetCodec<E> extends StringToCollectionCodec<E, Set<E>> {

  StringToSetCodec(
      TypeCodec<Set<E>> collectionCodec, ConvertingCodec<String, E> eltCodec, String delimiter) {
    super(collectionCodec, eltCodec, delimiter);
  }

  @NotNull
  @Override
  protected Supplier<Set<E>> collectionSupplier() {
    return LinkedHashSet::new;
  }
}
