/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.LoggerFactory;

class DefaultLogInterceptor implements LogInterceptor {

  private final String loggerName;
  private final Level level;
  private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();

  private Logger logger;
  private Level oldLevel;
  private Appender<ILoggingEvent> appender;
  private volatile boolean active;

  DefaultLogInterceptor(String loggerName, int level) {
    this.loggerName = loggerName;
    this.level = Level.fromLocationAwareLoggerInteger(level);
  }

  @Override
  public List<ILoggingEvent> getLoggedEvents() {
    return events;
  }

  @Override
  public void clear() {
    events.clear();
  }

  void start() {
    if (!active) {
      active = true;
      logger = (Logger) LoggerFactory.getLogger(loggerName);
      oldLevel = logger.getLevel();
      logger.setLevel(level);
      addTestAppender();
    }
  }

  void stop() {
    if (active) {
      active = false;
      logger.detachAppender(appender);
      logger.setLevel(oldLevel);
      clear();
    }
  }

  @SuppressWarnings("unchecked")
  private void addTestAppender() {
    appender = mock(Appender.class);
    logger.addAppender(appender);
    doAnswer(
            invocation -> {
              events.add(invocation.getArgument(0));
              return null;
            })
        .when(appender)
        .doAppend(any(ILoggingEvent.class));
  }
}
