/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import java.nio.charset.Charset;

/** */
public interface StdoutInterceptor {

  String getStdout();

  String getStdout(Charset charset);

  void clear();
}
