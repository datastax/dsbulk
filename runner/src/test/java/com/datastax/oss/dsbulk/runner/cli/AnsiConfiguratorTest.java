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
package com.datastax.oss.dsbulk.runner.cli;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class AnsiConfiguratorTest {

  @BeforeEach
  @AfterEach
  void clearProperties() {
    System.clearProperty("jansi.strip");
    System.clearProperty("jansi.force");
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_ansi_mode(
      List<String> args, String expectedJansiStrip, String expectedJansiForce)
      throws ParseException {

    AnsiConfigurator.configureAnsi(args);

    assertThat(System.getProperty("jansi.strip")).isEqualTo(expectedJansiStrip);
    assertThat(System.getProperty("jansi.force")).isEqualTo(expectedJansiForce);
  }

  static Stream<Arguments> should_detect_ansi_mode() {
    return Stream.of(
        Arguments.of(Arrays.asList("--log.ansiMode", "disabled"), "true", null),
        Arguments.of(Arrays.asList("--dsbulk.log.ansiMode", "disabled"), "true", null),
        Arguments.of(Arrays.asList("--log.ansiMode", "force"), null, "true"),
        Arguments.of(Arrays.asList("--dsbulk.log.ansiMode", "force"), null, "true"),
        Arguments.of(Collections.singletonList("--log.ansiMode=disabled"), "true", null),
        Arguments.of(Collections.singletonList("--dsbulk.log.ansiMode=disabled"), "true", null),
        Arguments.of(Collections.singletonList("--log.ansiMode=force"), null, "true"),
        Arguments.of(Collections.singletonList("--dsbulk.log.ansiMode=force"), null, "true"));
  }

  @ParameterizedTest
  @MethodSource
  void should_fail_when_incorrect_config(List<String> args, String expectedMessage) {
    Throwable t = catchThrowable(() -> AnsiConfigurator.configureAnsi(args));
    assertThat(t).isInstanceOf(ParseException.class).hasMessageContaining(expectedMessage);
  }

  static Stream<Arguments> should_fail_when_incorrect_config() {
    return Stream.of(
        // missing value
        Arguments.of(
            Collections.singletonList("--log.ansiMode"), "Expecting value after: --log.ansiMode"),
        Arguments.of(
            Collections.singletonList("--dsbulk.log.ansiMode"),
            "Expecting value after: --dsbulk.log.ansiMode"),
        // invalid value
        Arguments.of(
            Arrays.asList("--log.ansiMode", "INCORRECT"), "Invalid value for --log.ansiMode"),
        Arguments.of(
            Arrays.asList("--dsbulk.log.ansiMode", "INCORRECT"),
            "Invalid value for --dsbulk.log.ansiMode"),
        // invalid value with key=value syntax
        Arguments.of(
            Collections.singletonList("--log.ansiMode=INCORRECT"),
            "Invalid value for --log.ansiMode"),
        Arguments.of(
            Collections.singletonList("--dsbulk.log.ansiMode=INCORRECT"),
            "Invalid value for --dsbulk.log.ansiMode"));
  }
}
