/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.ConsoleAppender;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;

/**
 * An enhanced version of {@link ConsoleAppender} that selectively enables color codes handling with
 * the JANSI library.
 *
 * <p>Some Windows terminals like Mintty and MinGW are natively ANSI-compatible, but the JANSI
 * library does not handle them correctly, see <a
 * href=https://github.com/fusesource/jansi/issues/3">this issue</a>.
 *
 * <p>This appender works around this issue by only enabling the JANSI library if 1) we are on
 * Windows and 2) the terminal is not natively compatible with ANSI color codes.
 */
public class AnsiConsoleAppender<E extends ILoggingEvent> extends ConsoleAppender<E> {

  public AnsiConsoleAppender() {
    withJansi = !PlatformUtils.isAnsi() && PlatformUtils.isWindows();
  }
}
