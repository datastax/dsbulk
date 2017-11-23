/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.Context;
import ch.qos.logback.core.LogbackException;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;
import ch.qos.logback.core.status.Status;
import java.util.ArrayList;
import java.util.List;

public class TestAppender implements Appender<ILoggingEvent> {
  List<ILoggingEvent> events = new ArrayList<>();

  public String getName() {
    return "TestAppender";
  }

  @Override
  public void doAppend(ILoggingEvent event) throws LogbackException {
    events.add(event);
  }

  @Override
  public void setName(String name) {}

  @Override
  public void setContext(Context context) {}

  @Override
  public Context getContext() {
    return null;
  }

  @Override
  public void addStatus(Status status) {}

  @Override
  public void addInfo(String msg) {}

  @Override
  public void addInfo(String msg, Throwable ex) {}

  @Override
  public void addWarn(String msg) {}

  @Override
  public void addWarn(String msg, Throwable ex) {}

  @Override
  public void addError(String msg) {}

  @Override
  public void addError(String msg, Throwable ex) {}

  @Override
  public void addFilter(Filter newFilter) {}

  @Override
  public void clearAllFilters() {}

  @Override
  public List<Filter<ILoggingEvent>> getCopyOfAttachedFiltersList() {
    return null;
  }

  @Override
  public FilterReply getFilterChainDecision(ILoggingEvent event) {
    return null;
  }

  @Override
  public void start() {}

  @Override
  public void stop() {}

  @Override
  public boolean isStarted() {
    return false;
  }

  public List<ILoggingEvent> getEvents() {
    return events;
  }
}
