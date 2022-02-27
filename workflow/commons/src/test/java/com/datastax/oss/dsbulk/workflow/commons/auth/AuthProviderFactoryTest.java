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
package com.datastax.oss.dsbulk.workflow.commons.auth;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.event.Level;

@ExtendWith(LogInterceptingExtension.class)
class AuthProviderFactoryTest {

  @Test
  void should_error_invalid_auth_provider() {
    Config config =
        TestConfigUtils.createTestConfig("dsbulk.driver.auth", "provider", "InvalidAuthProvider");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value for dsbulk.driver.auth.provider")
        .hasMessageContaining(
            "expecting one of PlainTextAuthProvider, DsePlainTextAuthProvider, or DseGSSAPIAuthProvider, got: 'InvalidAuthProvider'");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_username() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.auth", "provider", "PlainTextAuthProvider", "username", "\"\"");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Both dsbulk.driver.auth.username and dsbulk.driver.auth.password "
                + "must be provided with PlainTextAuthProvider");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_password() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.auth", "provider", "DsePlainTextAuthProvider", "password", "\"\"");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Both dsbulk.driver.auth.username and dsbulk.driver.auth.password "
                + "must be provided with DsePlainTextAuthProvider");
  }

  @Test
  void should_generate_deprecation_warning_when_client_uses_dse_plain_text_auth_provider(
      @LogCapture(level = Level.WARN) LogInterceptor logs) {
    // given
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.auth",
            "provider",
            "DsePlainTextAuthProvider",
            "password",
            "s3cr3t",
            "username",
            "u");

    // when
    AuthProviderFactory.createAuthProvider(config);

    // then
    assertThat(logs.getLoggedMessages())
        .contains(
            "The DsePlainTextAuthProvider is deprecated. Please use PlainTextAuthProvider instead.");
  }

  @Test
  void should_error_nonexistent_keytab() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.auth", "provider", "DseGSSAPIAuthProvider", "keyTab", "noexist.keytab");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching(".*Keytab file .*noexist.keytab does not exist.*");
  }

  @Test
  void should_error_keytab_is_a_dir() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.auth", "provider", "DseGSSAPIAuthProvider", "keyTab", "\".\"");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching(".*Keytab file .* is not a file.*");
  }

  @Test
  void should_error_keytab_has_no_keys() throws IOException {
    Path keytabPath = Files.createTempFile("my", ".keytab");
    try {
      Config config =
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.auth",
              "provider",
              "DseGSSAPIAuthProvider",
              "keyTab",
              StringUtils.quoteJson(keytabPath));
      assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageMatching(".*Could not find any principals in.*");
    } finally {
      Files.delete(keytabPath);
    }
  }

  @Test
  void should_error_DseGSSAPIAuthProvider_and_no_sasl_protocol() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.auth", "provider", "DseGSSAPIAuthProvider", "saslService", null);
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "dsbulk.driver.auth.saslService must be provided with DseGSSAPIAuthProvider. "
                + "dsbulk.driver.auth.principal, dsbulk.driver.auth.keyTab, and dsbulk.driver.auth.authorizationId are optional.");
  }
}
