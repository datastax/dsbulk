/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.logging;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

public interface StreamInterceptor {

  default String getStreamAsString() {
    return getStreamAsString(StandardCharsets.UTF_8);
  }

  String getStreamAsString(Charset charset);

  default List<String> getStreamLines() {
    return getStreamLines(StandardCharsets.UTF_8);
  }

  default List<String> getStreamLines(Charset charset) {
    return Arrays.asList(getStreamAsString(charset).split(System.lineSeparator()));
  }

  void clear();
}
