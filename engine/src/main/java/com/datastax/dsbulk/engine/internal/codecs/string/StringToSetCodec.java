/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToCollectionCodec;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Set;

public class StringToSetCodec<E> extends StringToCollectionCodec<E, Set<E>> {

  public StringToSetCodec(
      JsonNodeToCollectionCodec<E, Set<E>> jsonCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(jsonCodec, objectMapper, nullStrings);
  }
}
