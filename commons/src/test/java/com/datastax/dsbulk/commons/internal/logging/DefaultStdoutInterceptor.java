/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import com.datastax.dsbulk.commons.internal.utils.URLUtils;
import com.google.common.base.Charsets;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

/** */
public class DefaultStdoutInterceptor implements StdoutInterceptor {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();
  private PrintStream originalStdout;

  @Override
  public String getStdout() {
    return getStdout(Charsets.UTF_8);
  }

  @Override
  public String getStdout(Charset charset) {
    return new String(baos.toByteArray(), charset);
  }

  @Override
  public void clear() {
    baos.reset();
  }

  void start() {
    originalStdout = System.out;
    System.setOut(new PrintStream(baos));
  }

  void stop() {
    System.setOut(originalStdout);
    clear();
  }
}
