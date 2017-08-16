/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.codecs.CodecUtils.trimToNull;

import com.datastax.driver.core.TypeCodec;
import com.google.common.base.Splitter;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

public abstract class StringToCollectionCodec<E, C extends Collection<E>>
    extends ConvertingCodec<String, C> {

  private final ConvertingCodec<String, E> eltCodec;
  private final String delimiter;

  public StringToCollectionCodec(
      TypeCodec<C> collectionCodec, ConvertingCodec<String, E> eltCodec, String delimiter) {
    super(collectionCodec, String.class);
    this.delimiter = delimiter;
    this.eltCodec = eltCodec;
  }

  @Override
  protected String convertTo(C value) {
    if (value == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Iterator<E> it = value.iterator(); it.hasNext(); ) {
      String s = eltCodec.convertTo(it.next());
      if (s != null) {
        sb.append(s);
      }
      if (it.hasNext()) {
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }

  @Override
  protected C convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    return StreamSupport.stream(Splitter.on(delimiter).trimResults().split(s).spliterator(), false)
        .map(token -> eltCodec.convertFrom(trimToNull(token)))
        .collect(Collectors.toCollection(collectionSupplier()));
  }

  @NotNull
  protected abstract Supplier<C> collectionSupplier();
}
