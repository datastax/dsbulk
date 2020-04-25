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
package com.datastax.oss.dsbulk.tests.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
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
    Optional<String> message =
        actual.getLoggedMessages().stream().filter(s -> s.contains(fragment)).findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to have a message containing '%s' "
                + "but they did not. Logged messages are: \n%s",
            fragment, String.join("\n", actual.getLoggedMessages()))
        .isPresent();
    return this;
  }

  public LogInterceptorAssert doesNotHaveMessageContaining(String fragment) {
    Optional<String> message =
        actual.getLoggedMessages().stream().filter(s -> s.contains(fragment)).findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to not have a message containing '%s' "
                + "but they did. Logged messages are: \n%s",
            fragment, String.join("\n", actual.getLoggedMessages()))
        .isNotPresent();
    return this;
  }

  public LogInterceptorAssert hasMessageMatching(String regex) {
    Optional<String> message =
        actual.getLoggedMessages().stream()
            .filter(s -> Pattern.compile(regex).matcher(s).find())
            .findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to have a message matching '%s' "
                + "but they did not. Logged messages are: \n%s",
            regex, String.join("\n", actual.getLoggedMessages()))
        .isPresent();
    return this;
  }

  public LogInterceptorAssert doesNotHaveMessageMatching(String regex) {
    Optional<String> message =
        actual.getLoggedMessages().stream()
            .filter(s -> Pattern.compile(regex).matcher(s).find())
            .findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to not have a message matching '%s' "
                + "but they did. Logged messages are: \n%s",
            regex, String.join("\n", actual.getLoggedMessages()))
        .isNotPresent();
    return this;
  }

  public LogInterceptorAssert hasMessageSatisfying(Predicate<String> predicate) {
    Optional<String> message = actual.getLoggedMessages().stream().filter(predicate).findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to have a satisfying message "
                + "but they did not. Logged messages are: \n%s",
            String.join("\n", actual.getLoggedMessages()))
        .isPresent();
    return this;
  }

  public LogInterceptorAssert hasEventSatisfying(Predicate<ILoggingEvent> predicate) {
    Optional<ILoggingEvent> message = actual.getLoggedEvents().stream().filter(predicate).findAny();
    assertThat(message)
        .overridingErrorMessage(
            "Expecting logged messages to have a satisfying message "
                + "but they did not. Logged messages are: \n%s",
            String.join("\n", actual.getLoggedMessages()))
        .isPresent();
    return this;
  }
}
