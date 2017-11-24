/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;

public class RxJavaCqlScriptReaderTest extends ReactiveCqlScriptReaderTestBase {

  @Override
  protected RxJavaCqlScriptReader getCqlScriptReader(String resource, boolean multiLine)
      throws IOException {
    URL url = Resources.getResource(resource);
    return new RxJavaCqlScriptReader(
        Resources.asCharSource(url, UTF_8).openBufferedStream(), multiLine);
  }
}
