/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;

public class StringToListCodec<E> extends StringToCollectionCodec<E, List<E>> {

  StringToListCodec(
      TypeCodec<List<E>> collectionCodec, ConvertingCodec<String, E> eltCodec, String delimiter) {
    super(collectionCodec, eltCodec, delimiter);
  }

  @NotNull
  @Override
  protected Supplier<List<E>> collectionSupplier() {
    return ArrayList::new;
  }
}
