/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import static ch.qos.logback.core.spi.FilterReply.DENY;
import static ch.qos.logback.core.spi.FilterReply.NEUTRAL;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.slf4j.Logger.ROOT_LOGGER_NAME;
import static org.slf4j.event.EventConstants.INFO_INT;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.LoggerFactory;

class DefaultLogInterceptor implements LogInterceptor {

  private final String loggerName;
  private final Level level;
  private final List<ILoggingEvent> events = new CopyOnWriteArrayList<>();
  private final AppenderInterceptingFilter interceptingFilter;

  private Logger logger;
  private Level oldLevel;
  private Appender<ILoggingEvent> appender;
  private volatile boolean active;

  DefaultLogInterceptor() {
    this(ROOT_LOGGER_NAME, INFO_INT);
  }

  DefaultLogInterceptor(String loggerName, int level) {
    this.loggerName = loggerName;
    this.level = Level.fromLocationAwareLoggerInteger(level);
    interceptingFilter = new AppenderInterceptingFilter();
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
      interceptRootAppenders();
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

  private void interceptRootAppenders() {
    Iterator<Appender<ILoggingEvent>> it =
        ((Logger) LoggerFactory.getLogger(ROOT_LOGGER_NAME)).iteratorForAppenders();
    while (it.hasNext()) {
      Appender<ILoggingEvent> appender = it.next();
      appender.addFilter(interceptingFilter);
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

  private class AppenderInterceptingFilter extends Filter<ILoggingEvent> {
    @Override
    public FilterReply decide(ILoggingEvent event) {
      // intercept log events that should not arrive here under normal circumstances
      if (active && event.getLevel().toInt() < oldLevel.levelInt) {
        return DENY;
      }
      return NEUTRAL;
    }
  }
}
