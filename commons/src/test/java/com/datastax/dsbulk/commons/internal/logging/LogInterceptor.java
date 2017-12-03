/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.util.List;
import java.util.stream.Collectors;

/** */
public interface LogInterceptor {

  List<ILoggingEvent> getLoggedEvents();

  default List<String> getLoggedMessages() {
    return getLoggedEvents()
        .stream()
        .map(ILoggingEvent::getFormattedMessage)
        .collect(Collectors.toList());
  }

  default String getAllMessagesAsString() {
    return getLoggedMessages().stream().collect(Collectors.joining(System.lineSeparator()));
  }

  void clear();
}
