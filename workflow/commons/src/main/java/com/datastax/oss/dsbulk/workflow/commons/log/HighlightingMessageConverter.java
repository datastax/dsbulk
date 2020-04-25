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
package com.datastax.oss.dsbulk.workflow.commons.log;

import static org.fusesource.jansi.Ansi.ansi;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.pattern.MessageConverter;
import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * A {@link MessageConverter} that highlights WARN and ERROR messages in bold yellow and bold red
 * respectively.
 */
public class HighlightingMessageConverter extends MessageConverter {

  @Override
  public String convert(ILoggingEvent event) {
    String msg = super.convert(event);
    if (event.getLevel().toInt() == Level.WARN_INT) {
      msg = ansi().bold().fgYellow().a(event.getFormattedMessage()).reset().toString();
    }
    if (event.getLevel().toInt() >= Level.ERROR_INT) {
      msg = ansi().bold().fgRed().a(event.getFormattedMessage()).reset().toString();
    }
    return msg;
  }
}
