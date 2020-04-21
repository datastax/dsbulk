/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.ssl;

import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import org.junit.jupiter.api.Test;

class SslHandlerFactoryFactoryTest {

  @Test
  void should_accept_existing_jdk_keystore() throws IOException, GeneralSecurityException {
    Path key = DEFAULT_CLIENT_KEYSTORE_FILE.toPath();
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "keystore.path",
            quoteJson(key),
            "keystore.password",
            DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    SslHandlerFactoryFactory.createSslHandlerFactory(config);
  }

  @Test
  void should_accept_existing_jdk_truststore() throws IOException, GeneralSecurityException {
    Path key = DEFAULT_CLIENT_TRUSTSTORE_FILE.toPath();
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "truststore.path",
            quoteJson(key),
            "truststore.password",
            DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
    SslHandlerFactoryFactory.createSslHandlerFactory(config);
  }

  @Test
  void should_accept_existing_openssl_keycertchain_and_key()
      throws IOException, GeneralSecurityException {
    Path key = DEFAULT_CLIENT_PRIVATE_KEY_FILE.toPath();
    Path chain = DEFAULT_CLIENT_CERT_CHAIN_FILE.toPath();
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "OpenSSL",
            "openssl.keyCertChain",
            quoteJson(chain),
            "openssl.privateKey",
            quoteJson(key));
    SslHandlerFactoryFactory.createSslHandlerFactory(config);
  }

  @Test
  void should_error_keystore_without_password() throws IOException {
    Path keystore = Files.createTempFile("my", "keystore");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl", "provider", "JDK", "keystore.path", "" + quoteJson(keystore));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "dsbulk.driver.ssl.keystore.path, dsbulk.driver.ssl.keystore.password and dsbulk.driver.ssl.truststore.algorithm must be provided together");
    } finally {
      Files.delete(keystore);
    }
  }

  @Test
  void should_error_password_without_keystore() {
    Config config =
        createTestConfig("dsbulk.driver.ssl", "provider", "JDK", "keystore.password", "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "dsbulk.driver.ssl.keystore.path, dsbulk.driver.ssl.keystore.password and dsbulk.driver.ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_openssl_keycertchain_without_key() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl", "provider", "OpenSSL", "openssl.keyCertChain", quoteJson(chain));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "dsbulk.driver.ssl.openssl.keyCertChain and "
                  + "dsbulk.driver.ssl.openssl.privateKey must be provided together");
    } finally {
      Files.delete(chain);
    }
  }

  @Test
  void should_error_key_without_openssl_keycertchain() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl", "provider", "OpenSSL", "openssl.privateKey", quoteJson(key));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "dsbulk.driver.ssl.openssl.keyCertChain and "
                  + "dsbulk.driver.ssl.openssl.privateKey must be provided together");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_error_truststore_without_password() throws IOException {
    Path truststore = Files.createTempFile("my", "truststore");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl", "provider", "JDK", "truststore.path", quoteJson(truststore));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageContaining(
              "dsbulk.driver.ssl.truststore.path, "
                  + "dsbulk.driver.ssl.truststore.password and "
                  + "dsbulk.driver.ssl.truststore.algorithm must be provided");
    } finally {
      Files.delete(truststore);
    }
  }

  @Test
  void should_error_password_without_truststore() {
    Config config =
        createTestConfig("dsbulk.driver.ssl", "provider", "JDK", "truststore.password", "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "dsbulk.driver.ssl.truststore.path, "
                + "dsbulk.driver.ssl.truststore.password and "
                + "dsbulk.driver.ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_nonexistent_truststore() {
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "truststore.path",
            "noexist.truststore",
            "truststore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL truststore file .*noexist.truststore does not exist.*");
  }

  @Test
  void should_error_truststore_is_a_dir() {
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "truststore.path",
            "\".\"",
            "truststore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL truststore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_truststore() throws IOException, GeneralSecurityException {
    Path truststore = Files.createTempFile("my", ".truststore");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl",
              "truststore.password",
              "mypass",
              "truststore.path",
              quoteJson(truststore));
      SslHandlerFactoryFactory.createSslHandlerFactory(config);
    } finally {
      Files.delete(truststore);
    }
  }

  @Test
  void should_error_nonexistent_keystore() {
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "keystore.path",
            "noexist.keystore",
            "keystore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL keystore file .*noexist.keystore does not exist.*");
  }

  @Test
  void should_error_keystore_is_a_dir() {
    Config config =
        createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "keystore.path",
            "\".\"",
            "keystore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageMatching(".*SSL keystore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_keystore() throws IOException, GeneralSecurityException {
    Path keystore = Files.createTempFile("my", ".keystore");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl",
              "keystore.password",
              "mypass",
              "keystore.path",
              quoteJson(keystore));
      SslHandlerFactoryFactory.createSslHandlerFactory(config);
    } finally {
      Files.delete(keystore);
    }
  }

  @Test
  void should_error_nonexistent_openssl_keycertchain() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.keyCertChain",
              "noexist.chain",
              "openssl.privateKey",
              quoteJson(key));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(
              ".*OpenSSL key certificate chain file .*noexist.chain does not exist.*");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_error_openssl_keycertchain_is_a_dir() throws IOException {
    Path key = Files.createTempFile("my", ".key");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.keyCertChain",
              "\".\"",
              "openssl.privateKey",
              quoteJson(key));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL key certificate chain file .* is not a file.*");
    } finally {
      Files.delete(key);
    }
  }

  @Test
  void should_error_nonexistent_openssl_key() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.privateKey",
              "noexist.key",
              "openssl.keyCertChain",
              quoteJson(chain));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL private key file .*noexist.key does not exist.*");
    } finally {
      Files.delete(chain);
    }
  }

  @Test
  void should_error_openssl_key_is_a_dir() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      Config config =
          createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "" + "openssl.privateKey",
              "\".\"",
              "" + "openssl.keyCertChain",
              "" + quoteJson(chain));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(BulkConfigurationException.class)
          .hasMessageMatching(".*OpenSSL private key file .* is not a file.*");
    } finally {
      Files.delete(chain);
    }
  }
}
