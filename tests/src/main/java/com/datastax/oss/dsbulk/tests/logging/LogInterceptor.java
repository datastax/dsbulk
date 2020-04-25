/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
