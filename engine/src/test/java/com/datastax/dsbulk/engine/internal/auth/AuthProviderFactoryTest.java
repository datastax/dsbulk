/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.auth;

import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
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
    LoaderConfig config = createTestConfig("dsbulk.driver.auth", "provider", "InvalidAuthProvider");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid value for dsbulk.driver.auth.provider")
        .hasMessageContaining(
            "expecting one of PlainTextAuthProvider, DsePlainTextAuthProvider, or DseGSSAPIAuthProvider, got: 'InvalidAuthProvider'");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_username() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.driver.auth", "provider", "PlainTextAuthProvider", "username", "\"\"");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Both dsbulk.driver.auth.username and dsbulk.driver.auth.password "
                + "must be provided with PlainTextAuthProvider");
  }

  @Test
  void should_error_invalid_auth_combinations_missing_password() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.driver.auth", "provider", "DsePlainTextAuthProvider", "password", "\"\"");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Both dsbulk.driver.auth.username and dsbulk.driver.auth.password "
                + "must be provided with DsePlainTextAuthProvider");
  }

  @Test
  void should_generate_deprecation_warning_when_client_uses_dse_plain_text_auth_provider(
      @LogCapture(level = Level.WARN) LogInterceptor logs) {
    // given
    LoaderConfig config =
        createTestConfig(
            "dsbulk.driver.auth",
            "provider",
            "DsePlainTextAuthProvider",
            "password",
            "\"\"",
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
    LoaderConfig config =
        createTestConfig(
            "dsbulk.driver.auth", "provider", "DseGSSAPIAuthProvider", "keyTab", "noexist.keytab");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*Keytab file .*noexist.keytab does not exist.*");
  }

  @Test
  void should_error_keytab_is_a_dir() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.driver.auth", "provider", "DseGSSAPIAuthProvider", "keyTab", "\".\"");
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*Keytab file .* is not a file.*");
  }

  @Test
  void should_error_keytab_has_no_keys() throws IOException {
    Path keytabPath = Files.createTempFile("my", ".keytab");
    try {
      LoaderConfig config =
          createTestConfig(
              "dsbulk.driver.auth",
              "provider",
              "DseGSSAPIAuthProvider",
              "keyTab",
              quoteJson(keytabPath));
      assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*Could not find any principals in.*");
    } finally {
      Files.delete(keytabPath);
    }
  }

  @Test
  void should_error_DseGSSAPIAuthProvider_and_no_sasl_protocol() {
    LoaderConfig config =
        createTestConfig(
            "dsbulk.driver.auth", "provider", "DseGSSAPIAuthProvider", "saslService", null);
    assertThatThrownBy(() -> AuthProviderFactory.createAuthProvider(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "dsbulk.driver.auth.saslService must be provided with DseGSSAPIAuthProvider. "
                + "dsbulk.driver.auth.principal, dsbulk.driver.auth.keyTab, and dsbulk.driver.auth.authorizationId are optional.");
  }
}
