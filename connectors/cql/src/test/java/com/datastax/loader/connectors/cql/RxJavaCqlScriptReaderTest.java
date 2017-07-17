/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.cql;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;

import static java.nio.charset.StandardCharsets.UTF_8;

public class RxJavaCqlScriptReaderTest extends AbstractReactiveCqlScriptReaderTest {

  @Override
  protected RxJavaCqlScriptReader getCqlScriptReader(String resource, boolean multiLine)
      throws IOException {
    URL url = Resources.getResource(resource);
    return new RxJavaCqlScriptReader(
        Resources.asCharSource(url, UTF_8).openBufferedStream(), multiLine);
  }
}
