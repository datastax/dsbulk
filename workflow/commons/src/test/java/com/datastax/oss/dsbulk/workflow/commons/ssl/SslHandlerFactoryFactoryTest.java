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
package com.datastax.oss.dsbulk.workflow.commons.ssl;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.oss.dsbulk.tests.ccm.DefaultCCMCluster;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import org.junit.jupiter.api.Test;

class SslHandlerFactoryFactoryTest {

  @Test
  void should_accept_existing_jdk_keystore() throws IOException, GeneralSecurityException {
    Path key = DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_FILE.toPath();
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "keystore.path",
            StringUtils.quoteJson(key),
            "keystore.password",
            DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    SslHandlerFactoryFactory.createSslHandlerFactory(config);
  }

  @Test
  void should_accept_existing_jdk_truststore() throws IOException, GeneralSecurityException {
    Path key = DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_FILE.toPath();
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "truststore.path",
            StringUtils.quoteJson(key),
            "truststore.password",
            DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
    SslHandlerFactoryFactory.createSslHandlerFactory(config);
  }

  @Test
  void should_accept_existing_openssl_keycertchain_and_key()
      throws IOException, GeneralSecurityException {
    Path key = DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_FILE.toPath();
    Path chain = DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_FILE.toPath();
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "OpenSSL",
            "openssl.keyCertChain",
            StringUtils.quoteJson(chain),
            "openssl.privateKey",
            StringUtils.quoteJson(key));
    SslHandlerFactoryFactory.createSslHandlerFactory(config);
  }

  @Test
  void should_error_keystore_without_password() throws IOException {
    Path keystore = Files.createTempFile("my", "keystore");
    try {
      Config config =
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "JDK",
              "keystore.path",
              "" + StringUtils.quoteJson(keystore));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageContaining(
              "dsbulk.driver.ssl.keystore.path, dsbulk.driver.ssl.keystore.password and dsbulk.driver.ssl.keystore.algorithm must be provided together");
    } finally {
      Files.delete(keystore);
    }
  }

  @Test
  void should_error_password_without_keystore() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl", "provider", "JDK", "keystore.password", "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "dsbulk.driver.ssl.keystore.path, dsbulk.driver.ssl.keystore.password and dsbulk.driver.ssl.keystore.algorithm must be provided together");
  }

  @Test
  void should_error_openssl_keycertchain_without_key() throws IOException {
    Path chain = Files.createTempFile("my", ".chain");
    try {
      Config config =
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.keyCertChain",
              StringUtils.quoteJson(chain));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
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
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.privateKey",
              StringUtils.quoteJson(key));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
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
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "JDK",
              "truststore.path",
              StringUtils.quoteJson(truststore));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
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
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl", "provider", "JDK", "truststore.password", "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "dsbulk.driver.ssl.truststore.path, "
                + "dsbulk.driver.ssl.truststore.password and "
                + "dsbulk.driver.ssl.truststore.algorithm must be provided together");
  }

  @Test
  void should_error_nonexistent_truststore() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "truststore.path",
            "noexist.truststore",
            "truststore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching(".*SSL truststore file .*noexist.truststore does not exist.*");
  }

  @Test
  void should_error_truststore_is_a_dir() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "truststore.path",
            "\".\"",
            "truststore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching(".*SSL truststore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_truststore() throws IOException, GeneralSecurityException {
    Path truststore = Files.createTempFile("my", ".truststore");
    try {
      Config config =
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "truststore.password",
              "mypass",
              "truststore.path",
              StringUtils.quoteJson(truststore));
      SslHandlerFactoryFactory.createSslHandlerFactory(config);
    } finally {
      Files.delete(truststore);
    }
  }

  @Test
  void should_error_nonexistent_keystore() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "keystore.path",
            "noexist.keystore",
            "keystore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching(".*SSL keystore file .*noexist.keystore does not exist.*");
  }

  @Test
  void should_error_keystore_is_a_dir() {
    Config config =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver.ssl",
            "provider",
            "JDK",
            "keystore.path",
            "\".\"",
            "keystore.password",
            "mypass");
    assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageMatching(".*SSL keystore file .* is not a file.*");
  }

  @Test
  void should_accept_existing_keystore() throws IOException, GeneralSecurityException {
    Path keystore = Files.createTempFile("my", ".keystore");
    try {
      Config config =
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "keystore.password",
              "mypass",
              "keystore.path",
              StringUtils.quoteJson(keystore));
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
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.keyCertChain",
              "noexist.chain",
              "openssl.privateKey",
              StringUtils.quoteJson(key));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
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
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.keyCertChain",
              "\".\"",
              "openssl.privateKey",
              StringUtils.quoteJson(key));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
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
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "openssl.privateKey",
              "noexist.key",
              "openssl.keyCertChain",
              StringUtils.quoteJson(chain));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
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
          TestConfigUtils.createTestConfig(
              "dsbulk.driver.ssl",
              "provider",
              "OpenSSL",
              "" + "openssl.privateKey",
              "\".\"",
              "" + "openssl.keyCertChain",
              "" + StringUtils.quoteJson(chain));
      assertThatThrownBy(() -> SslHandlerFactoryFactory.createSslHandlerFactory(config))
          .isInstanceOf(IllegalArgumentException.class)
          .hasMessageMatching(".*OpenSSL private key file .* is not a file.*");
    } finally {
      Files.delete(chain);
    }
  }
}
