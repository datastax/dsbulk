/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import ch.qos.logback.core.OutputStreamAppender;
import com.datastax.dsbulk.commons.url.UncloseablePrintStream;
import java.io.IOException;
import java.io.OutputStream;
import org.fusesource.jansi.AnsiConsole;

/**
 * An enhanced version of Logback's {@code ConsoleAppender} that wraps System.out and System.err in
 * a {@link org.fusesource.jansi.AnsiPrintStream} for consistent, cross-platform handling of ANSI
 * escape sequences.
 */
public class JansiConsoleAppender<E> extends OutputStreamAppender<E> {

  private enum ConsoleTarget {
    STDOUT,
    STDERR
  }

  private ConsoleTarget target;

  /**
   * Sets the value of the <b>Target</b> option. Recognized values are "STDOUT" and "STDERR". Any
   * other value will result in an error.
   *
   * @param value the value of the <b>Target</b> option.
   */
  @SuppressWarnings("unused")
  public void setTarget(String value) {
    target = ConsoleTarget.valueOf(value.trim());
  }

  @Override
  public void start() {
    switch (target) {
      case STDOUT:
        setOutputStream(
            AnsiConsole.wrapSystemOut(new UncloseablePrintStream(new SystemOutOutputStream())));
        break;
      case STDERR:
        setOutputStream(
            AnsiConsole.wrapSystemErr(new UncloseablePrintStream(new SystemErrOutputStream())));
        break;
    }
    super.start();
  }

  /*
  The classes below are wrappers around System.out and System.err.
  Note that we avoid holding references to these fields, in case they get redirected
  after this appender has started.
  */

  private static class SystemOutOutputStream extends OutputStream {

    @Override
    public void write(int b) {
      System.out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      System.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      System.out.write(b, off, len);
    }

    @Override
    public void flush() {
      System.out.flush();
    }
  }

  private static class SystemErrOutputStream extends OutputStream {

    @Override
    public void write(int b) {
      System.err.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
      System.err.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) {
      System.err.write(b, off, len);
    }

    @Override
    public void flush() {
      System.err.flush();
    }
  }
}
