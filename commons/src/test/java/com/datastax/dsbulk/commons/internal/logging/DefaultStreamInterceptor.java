/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import com.datastax.dsbulk.commons.internal.utils.URLUtils;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.Charset;

/** */
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
