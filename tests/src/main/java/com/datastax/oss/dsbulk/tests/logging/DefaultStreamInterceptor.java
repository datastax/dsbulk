/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.logging;

import com.datastax.oss.dsbulk.tests.utils.URLUtils;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

public class DefaultStreamInterceptor implements StreamInterceptor {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  private final StreamType streamType;
  private final ByteArrayOutputStream baos = new ByteArrayOutputStream();

  private PrintStream originalStream;
  private boolean started = false;

  DefaultStreamInterceptor(StreamType streamType) {
    this.streamType = streamType;
  }

  @Override
  public String getStreamAsString(Charset charset) {
    return new String(baos.toByteArray(), charset);
  }

  @Override
  public void clear() {
    baos.reset();
  }

  void start() {
    if (!started) {
      switch (streamType) {
        case STDOUT:
          originalStream = System.out;
          System.setOut(new PrintStream(baos));
          break;
        case STDERR:
          originalStream = System.err;
          System.setErr(new PrintStream(baos));
          break;
      }
    }
    started = true;
  }

  void stop() {
    if (started) {
      switch (streamType) {
        case STDOUT:
          System.setOut(originalStream);
          break;
        case STDERR:
          System.setErr(originalStream);
          break;
      }
      clear();
      started = false;
    }
  }
}
