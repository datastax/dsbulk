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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.slf4j.event.Level.INFO;
import static org.slf4j.event.Level.WARN;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.dsbulk.commons.utils.PlatformUtils;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import com.datastax.oss.dsbulk.tests.utils.TestConfigUtils;
import com.datastax.oss.dsbulk.tests.utils.URLUtils;
import com.typesafe.config.Config;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

@ExtendWith(LogInterceptingExtension.class)
class DriverSettingsTest {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  @ParameterizedTest
  @CsvSource({
    // IPv4 without port
    "192.168.0.1,9043,192.168.0.1:9043",
    // Unambiguous IPv6 without port (because Ipv6 literal is in full form)
    "fe80:0:0:0:f861:3eff:fe1d:9d7b,9043,fe80:0:0:0:f861:3eff:fe1d:9d7b:9043",
    "fe80:0:0:f861:3eff:fe1d:9d7b:9044,9043,fe80:0:0:f861:3eff:fe1d:9d7b:9044:9043",
    // Unambiguous IPv6 without port (because last block is not numeric)
    "fe80::f861:3eff:fe1d:9d7b,9043,fe80::f861:3eff:fe1d:9d7b:9043",
    // Ambiguous IPv6 without port
    "fe80::f861:3eff:fe1d:1234,9043,fe80::f861:3eff:fe1d:1234",
    // hostname without port
    "host.com,9043,host.com:9043",
    // IPv4 with port
    "192.168.0.1:9044,9043,192.168.0.1:9044",
    // Unambiguous IPv6 with port (because Ipv6 literal is in full form)
    "fe80:0:0:0:f861:3eff:fe1d:9d7b:9044,9043,fe80:0:0:0:f861:3eff:fe1d:9d7b:9044",
    "fe80:0:0:0:f861:3eff:fe1d:1234:9044,9043,fe80:0:0:0:f861:3eff:fe1d:1234:9044",
    // Unambiguous IPv6 with port (because last block is not numeric)
    "fe80::f861:3eff:fe1d:9d7b:9044,9043,fe80::f861:3eff:fe1d:9d7b:9044",
    // Ambiguous IPv6 with port
    "fe80::f861:3eff:fe1d:1234:9044,9043,fe80::f861:3eff:fe1d:1234:9044",
    // hostname with port
    "host.com:9044,9043,host.com:9044",
  })
  void should_crate_valid_contact_points(String host, int port, String expected)
      throws GeneralSecurityException, IOException {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.contact-points",
            "[" + StringUtils.quoteJson(host) + "]",
            "basic.default-port",
            port);
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points"))
        .containsExactly(expected);
  }

  @ParameterizedTest
  @CsvSource({
    // IPv4
    "192.168.0.1,9043,192.168.0.1:9043",
    // IPv6
    "fe80:0:0:0:f861:3eff:fe1d:9d7b,9043,fe80:0:0:0:f861:3eff:fe1d:9d7b:9043",
    "fe80:0:0:f861:3eff:fe1d:9d7b:9044,9043,fe80:0:0:f861:3eff:fe1d:9d7b:9044:9043",
    "fe80::f861:3eff:fe1d:9d7b,9043,fe80::f861:3eff:fe1d:9d7b:9043",
    "fe80::f861:3eff:fe1d:1234,9043,fe80::f861:3eff:fe1d:1234:9043",
    // hostname
    "host.com,9043,host.com:9043"
  })
  void should_crate_valid_contact_points_with_deprecated_settings(
      String host, int port, String expected) throws GeneralSecurityException, IOException {
    Config oldConfig =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver", "hosts", "[" + StringUtils.quoteJson(host) + "]", "port", port);
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points"))
        .containsExactly(expected);
  }

  @Test
  void should_throw_exception_when_port_not_a_number() {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver", "port", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value for dsbulk.driver.port, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_local_connections_not_a_number() {
    Config oldConfig =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver", "pooling.local.connections", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.local.connections, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_remote_connections_not_a_number() {
    Config oldConfig =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver", "pooling.remote.connections", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.remote.connections, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_local_requests_not_a_number() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "pooling.local.requests", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.local.requests, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_remote_requests_not_a_number() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "pooling.remote.requests", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.remote.requests, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_fetch_size_not_a_number() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "query.fetchSize", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.fetchSize, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_read_timeout_not_a_duration() {
    Config oldConfig =
        TestConfigUtils.createTestConfig(
            "dsbulk.driver", "socket.readTimeout", "\"I am not a duration\"");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid value for dsbulk.driver.socket.readTimeout")
        .hasMessageContaining("Could not parse time unit 'duration' (try ns, us, ms, s, m, h, d)");
  }

  @Test
  void should_throw_exception_when_max_retries_not_a_number() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "policy.maxRetries", "NotANumber");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.policy.maxRetries, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_idempotence_not_a_boolean() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "query.idempotence", "NotABoolean");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.idempotence, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_timestamp_generator_invalid() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "timestampGenerator", "Unknown");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.protocol.timestampGenerator, "
                + "expecting one of AtomicMonotonicTimestampGenerator, "
                + "ThreadLocalTimestampGenerator, ServerSideTimestampGenerator, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_address_translator_invalid() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "addressTranslator", "Unknown");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.protocol.addressTranslator, expecting IdentityTranslator, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_compression_invalid() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "protocol.compression", "Unknown");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.protocol.compression, expecting one of NONE, SNAPPY, LZ4, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_consistency_invalid() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "query.consistency", "Unknown");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.consistency, expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, got: 'Unknown'");
  }

  @Test
  void should_throw_exception_when_serial_consistency_invalid() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "query.serialConsistency", "Unknown");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.serialConsistency, expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, got: 'Unknown'");
  }

  @Test
  void should_throw_exception_when_heartbeat_invalid() {
    Config oldConfig =
        TestConfigUtils.createTestConfig("dsbulk.driver", "pooling.heartbeat", "NotADuration");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.heartbeat: No number in duration value 'NotADuration'");
  }

  @ParameterizedTest
  @CsvSource({
    "protocol.compression,LZ4,advanced.protocol.compression",
    "pooling.local.connections,1234,advanced.connection.pool.local.size",
    "pooling.remote.connections,1234,advanced.connection.pool.remote.size",
    "pooling.local.requests,1234,advanced.connection.max-requests-per-connection",
    "pooling.remote.requests,1234,advanced.connection.max-requests-per-connection",
    "pooling.heartbeat,30 seconds,advanced.heartbeat.interval",
    "query.serialConsistency,SERIAL,basic.request.serial-consistency",
    "query.fetchSize,1000,basic.request.page-size",
    "query.idempotence,true,basic.request.default-idempotence",
    "socket.readTimeout,60 seconds,basic.request.timeout",
    "timestampGenerator,AtomicMonotonicTimestampGenerator,advanced.timestamp-generator.class",
    "addressTranslator,IdentityTranslator,advanced.address-translator.class",
    "policy.lbp.whiteList.hosts,[127.0.0.1],basic.load-balancing-policy.filter.class"
  })
  void should_log_warning_when_deprecated_driver_setting_present(
      String deprecatedSetting,
      String value,
      String newSetting,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver", deprecatedSetting, value);
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.driver.%s is deprecated and will be removed in a future release; "
                    + "please configure the driver directly using --datastax-java-driver.%s instead.",
                deprecatedSetting, newSetting));
  }

  @ParameterizedTest
  @CsvSource({
    "hosts,[host.com],basic.contact-points,h",
    "port,9042,basic.default-port,port",
    "query.consistency,ONE,basic.request.consistency,cl",
    "policy.maxRetries,100,advanced.retry-policy.max-retries,maxRetries",
    "policy.lbp.dcAwareRoundRobin.localDc,testDC,basic.load-balancing-policy.local-datacenter,dc"
  })
  void should_log_warning_when_deprecated_driver_setting_present_with_shortcut(
      String deprecatedSetting,
      String value,
      String newSetting,
      String shortcut,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver", deprecatedSetting, value);
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.driver.%s is deprecated and will be removed in a future release; "
                    + "please configure the driver directly using --datastax-java-driver.%s (or -%s) instead.",
                deprecatedSetting, newSetting, shortcut));
  }

  @ParameterizedTest
  @CsvSource({
    "pageSize,5000,advanced.continuous-paging.page-size",
    "pageUnit,BYTES,advanced.continuous-paging.page-size-in-bytes",
    "maxPages,0,advanced.continuous-paging.max-pages",
    "maxPagesPerSecond,0,advanced.continuous-paging.max-pages-per-second",
  })
  void should_log_warning_when_deprecated_continuous_paging_setting_present(
      String deprecatedSetting,
      String value,
      String newSetting,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig =
        TestConfigUtils.createTestConfig(
            "dsbulk.executor.continuousPaging", deprecatedSetting, value);
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(false);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.executor.continuousPaging.%s is deprecated and will be removed in a future release; "
                    + "please configure the driver directly using --datastax-java-driver.%s instead.",
                deprecatedSetting, newSetting));
  }

  @ParameterizedTest
  @CsvSource({
    "policy.lbp.name,FooPolicy",
    "policy.lbp.dse.childPolicy,tokenAware",
    "policy.lbp.tokenAware.childPolicy,roundRobin",
    "policy.lbp.tokenAware.replicaOrdering,NEUTRAL",
    "policy.lbp.dcAwareRoundRobin.allowRemoteDCsForLocalConsistencyLevel,true",
    "policy.lbp.dcAwareRoundRobin.usedHostsPerRemoteDc,1",
    "policy.lbp.whiteList.childPolicy,roundRobin",
  })
  void should_log_warning_when_obsolete_LBP_setting_present(
      String obsoleteSetting,
      String value,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver", obsoleteSetting, value);
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.driver.%s has been removed and is not honored anymore; "
                    + "please remove it from your configuration. "
                    + "To configure the load balancing policy, use "
                    + "--datastax-java-driver.basic.load-balancing-policy.* instead",
                obsoleteSetting));
  }

  @Test
  void should_throw_exception_when_page_unit_invalid() {
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig =
        TestConfigUtils.createTestConfig(
            "dsbulk.executor.continuousPaging", "pageUnit", "NotAPageUnit");
    Config newConfig = TestConfigUtils.createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(false))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.executor.continuousPaging.pageUnit, expecting one of BYTES, ROWS, got 'NotAPageUnit'");
  }

  @ParameterizedTest
  @MethodSource
  void should_accept_cloud_secure_connect_bundle(String input, String expected)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.cloud.secure-connect-bundle",
            StringUtils.quoteJson(input));
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(settings.getDriverConfig().getString("basic.cloud.secure-connect-bundle"))
        .isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  private static Stream<Arguments> should_accept_cloud_secure_connect_bundle()
      throws MalformedURLException {
    return Stream.of(
        Arguments.of("/tmp/bundle.zip", "file:/tmp/bundle.zip"),
        Arguments.of(
            "./bundle.zip",
            Paths.get(System.getProperty("user.dir"))
                .resolve("bundle.zip")
                .toUri()
                .toURL()
                .toExternalForm()),
        Arguments.of("-", "std:/"),
        Arguments.of(
            "~/bundle.zip",
            Paths.get(System.getProperty("user.home"))
                .resolve("bundle.zip")
                .toUri()
                .toURL()
                .toExternalForm()),
        Arguments.of("file:/tmp/bundle.zip", "file:/tmp/bundle.zip"),
        Arguments.of("file:/tmp/../tmp/bundle.zip", "file:/tmp/bundle.zip"),
        Arguments.of(
            "file:./bundle.zip",
            Paths.get(System.getProperty("user.dir"))
                .resolve("bundle.zip")
                .toUri()
                .toURL()
                .toExternalForm()),
        Arguments.of("http://host.com/bundle.zip", "http://host.com/bundle.zip"),
        Arguments.of("https://host.com/bundle.zip", "https://host.com/bundle.zip"),
        Arguments.of("ftp://host.com/bundle.zip", "ftp://host.com/bundle.zip"),
        Arguments.of("std:/", "std:/"));
  }

  @Test
  void should_log_info_when_cloud_and_implicit_contact_points(
      @LogCapture(level = INFO, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver", "basic.cloud.secure-connect-bundle", "/path/to/bundle");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "A cloud secure connect bundle was provided: ignoring all explicit contact points.");
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points")).isEmpty();
  }

  @Test
  void should_log_warn_when_cloud_and_explicit_contact_points(
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.contact-points",
            "[\"host.com:9042\"]",
            "basic.cloud.secure-connect-bundle",
            "/path/to/bundle");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "Explicit contact points provided together with a cloud secure connect bundle; "
                + "the following contact points will be ignored: \"host.com:9042\"");
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points")).isEmpty();
  }

  @Test
  void should_log_warn_when_cloud_and_explicit_contact_points_legacy(
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver", "hosts", "[host.com]");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver", "basic.cloud.secure-connect-bundle", "/path/to/bundle");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "Explicit contact points provided together with a cloud secure connect bundle; "
                + "the following contact points will be ignored: \"host.com:9042\"");
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points")).isEmpty();
  }

  @Test
  void should_log_info_when_cloud_and_write_and_default_CL_implicitly_changed(
      @LogCapture(level = INFO, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver", "basic.cloud.secure-connect-bundle", "/path/to/bundle");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "A cloud secure connect bundle was provided and selected operation performs writes: "
                + "changing default consistency level to LOCAL_QUORUM.");
    assertThat(settings.getDriverConfig().getString("basic.request.consistency"))
        .isEqualTo("LOCAL_QUORUM");
  }

  @ParameterizedTest
  @CsvSource({"ANY", "LOCAL_ONE", "ONE"})
  void should_log_warning_when_cloud_and_write_and_incompatible_CL_explicitly_set(
      DefaultConsistencyLevel level,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.cloud.secure-connect-bundle",
            "/path/to/bundle",
            "basic.request.consistency",
            level);
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "A cloud secure connect bundle was provided together with consistency level %s, "
                    + "but selected operation performs writes: "
                    + "forcing default consistency level to LOCAL_QUORUM.",
                level));
    assertThat(settings.getDriverConfig().getString("basic.request.consistency"))
        .isEqualTo("LOCAL_QUORUM");
  }

  @ParameterizedTest
  @CsvSource({"TWO", "THREE", "LOCAL_QUORUM", "QUORUM", "EACH_QUORUM", "ALL"})
  void should_not_log_warning_when_cloud_and_write_and_compatible_CL_explicitly_set(
      DefaultConsistencyLevel level,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.cloud.secure-connect-bundle",
            "/path/to/bundle",
            "basic.request.consistency",
            level);
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs.getLoggedEvents()).isEmpty();
    assertThat(settings.getDriverConfig().getString("basic.request.consistency"))
        .isEqualTo(level.name());
  }

  @ParameterizedTest
  @CsvSource({
    "ANY",
    "LOCAL_ONE",
    "ONE",
    "TWO",
    "THREE",
    "LOCAL_QUORUM",
    "QUORUM",
    "EACH_QUORUM",
    "ALL"
  })
  void should_not_log_warning_when_cloud_and_read_and_any_CL_explicitly_set(
      DefaultConsistencyLevel level,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.cloud.secure-connect-bundle",
            "/path/to/bundle",
            "basic.request.consistency",
            level);
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(false);
    assertThat(logs.getLoggedEvents()).isEmpty();
    assertThat(settings.getDriverConfig().getString("basic.request.consistency"))
        .isEqualTo(level.name());
  }

  @Test
  void should_log_warning_when_when_cloud_and_SSL_explicitly_set(
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver",
            "basic.cloud.secure-connect-bundle",
            "/path/to/bundle",
            "advanced.ssl-engine-factory.class",
            "DefaultSslEngineFactory");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "Explicit SSL configuration provided together with a cloud secure connect bundle: "
                + "SSL settings will be ignored.");
    assertThat(settings.getDriverConfig().hasPath("advanced.ssl-engine-factory")).isFalse();
    assertThat(settings.sslHandlerFactory).isNull();
  }

  @Test
  void should_log_warning_when_when_cloud_and_SSL_explicitly_set_legacy(
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    Config oldConfig = TestConfigUtils.createTestConfig("dsbulk.driver", "ssl.provider", "JDK");
    Config cpConfig = TestConfigUtils.createTestConfig("dsbulk.executor.continuousPaging");
    Config newConfig =
        TestConfigUtils.createTestConfig(
            "datastax-java-driver", "basic.cloud.secure-connect-bundle", "/path/to/bundle");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "Setting dsbulk.driver.ssl.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using "
                + "--datastax-java-driver.advanced.ssl-engine-factory.* instead.")
        .hasMessageContaining(
            "Explicit SSL configuration provided together with a cloud secure connect bundle: "
                + "SSL settings will be ignored.");
    assertThat(settings.getDriverConfig().hasPath("advanced.ssl-engine-factory")).isFalse();
    assertThat(settings.sslHandlerFactory).isNull();
  }
}
