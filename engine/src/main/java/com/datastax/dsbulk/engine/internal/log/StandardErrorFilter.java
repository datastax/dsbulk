/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
