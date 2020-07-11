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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import java.io.Console;
import org.junit.jupiter.api.Test;

class PasswordPrompterTest {

  @Test
  void should_prompt_for_missing_passwords() {
    // given
    Console console = mock(Console.class);
    when(console.readPassword("Please input value for setting %s: ", "my.password2"))
        .thenReturn("fakePasswordForTests2".toCharArray());
    Config config =
        ConfigFactory.parseString(
            "my.username1 = alice, my.password1 = fakePasswordForTests1, my.username2 = bob",
            ConfigParseOptions.defaults().setOriginDescription("Initial config"));
    // when
    PasswordPrompter prompter =
        new PasswordPrompter(
            ImmutableMap.of(
                "my.username1", // username1 has its password already -> will not be prompted
                "my.password1",
                "my.username2", // username2 is missing its password -> will be prompted
                "my.password2",
                "my.username3", // not present at all in the config -> will not be prompted
                "my.password3"),
            console);
    config = prompter.postProcess(config);
    // then
    // should not override existing password
    assertThat(config.hasPath("my.password1")).isTrue();
    assertThat(config.getString("my.password1")).isEqualTo("fakePasswordForTests1");
    assertThat(config.getValue("my.password1").origin().description()).contains("Initial config");
    // should prompt for password when missing
    assertThat(config.hasPath("my.password2")).isTrue();
    assertThat(config.getString("my.password2")).isEqualTo("fakePasswordForTests2");
    assertThat(config.getValue("my.password2").origin().description()).isEqualTo("stdin");
    // should not prompt if corresponding setting is not present
    assertThat(config.hasPath("my.password3")).isFalse();
    verify(console).readPassword("Please input value for setting %s: ", "my.password2");
    verifyNoMoreInteractions(console);
  }
}
