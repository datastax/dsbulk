/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log;

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
