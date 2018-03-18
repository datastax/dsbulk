/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import org.assertj.core.api.AbstractObjectAssert;

@SuppressWarnings("UnusedReturnValue")
public class LogInterceptorAssert
    extends AbstractObjectAssert<LogInterceptorAssert, LogInterceptor> {

  LogInterceptorAssert(LogInterceptor logInterceptor) {
    super(logInterceptor, LogInterceptorAssert.class);
  }

  public LogInterceptorAssert hasMessageContaining(String fragment) {
    hasMessageSatisfying(s -> s.contains(fragment));
    return this;
  }

  public LogInterceptorAssert hasMessageMatching(String regex) {
    hasMessageSatisfying(s -> Pattern.compile(regex).matcher(s).find());
    return this;
  }

  public LogInterceptorAssert hasMessageSatisfying(Predicate<String> predicate) {
    Optional<String> message = actual.getLoggedMessages().stream().filter(predicate).findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to have a satisfying message but they did not")
        .isPresent();
    return this;
  }

  public LogInterceptorAssert hasEventSatisfying(Predicate<ILoggingEvent> predicate) {
    Optional<ILoggingEvent> message = actual.getLoggedEvents().stream().filter(predicate).findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to have a satisfying message but they did not")
        .isPresent();
    return this;
  }
}
