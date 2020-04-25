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
      addTestAppender();
    }
  }

  void stop() {
    if (active) {
      active = false;
      removeTestAppender();
      clear();
    }
  }

  @SuppressWarnings("unchecked")
  private void addTestAppender() {
    logger = (Logger) LoggerFactory.getLogger(loggerName);
    oldLevel = logger.getLevel();
    logger.setLevel(level);
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

  private void removeTestAppender() {
    logger.detachAppender(appender);
    logger.setLevel(oldLevel);
  }
}
