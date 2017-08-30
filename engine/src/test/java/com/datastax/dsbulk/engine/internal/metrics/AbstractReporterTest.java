/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.codahale.metrics.MetricRegistry;
import com.datastax.dsbulk.executor.api.listener.MetricsReportingExecutionListener;
import org.junit.After;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.LoggerFactory;

public abstract class AbstractReporterTest {

  @Mock private Appender<ILoggingEvent> mockAppender;
  private Appender<ILoggingEvent> stdout;
  private Level oldLevel;
  private Logger root;

  MetricRegistry registry;

  @Before
  public void prepareMocks() {
    MockitoAnnotations.initMocks(this);
    registry = new MetricRegistry();
    when(mockAppender.getName()).thenReturn("MOCK");
    Logger logger = (Logger) LoggerFactory.getLogger(getLoggerClass());
    logger.addAppender(mockAppender);
    oldLevel = logger.getLevel();
    logger.setLevel(Level.INFO);
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    stdout = root.getAppender("STDOUT");
    root.detachAppender(stdout);
  }

  @After
  public void restoreAppenders() {
    Logger logger = (Logger) LoggerFactory.getLogger(MetricsReportingExecutionListener.class);
    logger.detachAppender(mockAppender);
    logger.setLevel(oldLevel);
    root.addAppender(stdout);
  }

  abstract Class<?> getLoggerClass();

  void verifyEventLogged(String expectedLogMessage) {
    verify(mockAppender)
        .doAppend(
            argThat(
                new ArgumentMatcher<ILoggingEvent>() {
                  @Override
                  public boolean matches(final Object argument) {
                    return ((ILoggingEvent) argument)
                        .getFormattedMessage()
                        .contains(expectedLogMessage);
                  }
                }));
  }
}
