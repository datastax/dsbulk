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

public class StringToListCodec<E> extends StringToCollectionCodec<E, List<E>> {

  public StringToListCodec(
      JsonNodeToCollectionCodec<E, List<E>> jsonCodec,
      ObjectMapper objectMapper,
      List<String> nullStrings) {
    super(jsonCodec, objectMapper, nullStrings);
  }
}
