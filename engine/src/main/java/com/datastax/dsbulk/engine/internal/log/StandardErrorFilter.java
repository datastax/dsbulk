/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.filter.Filter;
import ch.qos.logback.core.spi.FilterReply;

public class StandardErrorFilter extends Filter<ILoggingEvent> {

  @Override
  public FilterReply decide(ILoggingEvent event) {
    if (!isStarted()) {
      return FilterReply.NEUTRAL;
    }
    if (event.getLevel().levelInt >= Level.ERROR_INT) {
      return FilterReply.NEUTRAL;
    } else {
      return FilterReply.DENY;
    }
  }
}
