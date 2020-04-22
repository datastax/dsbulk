/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.logging;

import ch.qos.logback.classic.spi.ILoggingEvent;
import java.util.List;
import java.util.stream.Collectors;

public interface LogInterceptor {

  List<ILoggingEvent> getLoggedEvents();

  default List<String> getLoggedMessages() {
    return getLoggedEvents().stream()
        .map(ILoggingEvent::getFormattedMessage)
        .collect(Collectors.toList());
  }

  default String getAllMessagesAsString() {
    return getLoggedMessages().stream().collect(Collectors.joining(System.lineSeparator()));
  }

  void clear();
}
