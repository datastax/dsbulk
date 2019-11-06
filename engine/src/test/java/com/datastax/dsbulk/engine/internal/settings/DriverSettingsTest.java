/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.commons.tests.utils.TestConfigUtils.createTestConfig;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.slf4j.event.Level.INFO;
import static org.slf4j.event.Level.WARN;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import java.io.IOException;
import java.security.GeneralSecurityException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

@ExtendWith(LogInterceptingExtension.class)
class DriverSettingsTest {

  @Test
  void should_crate_valid_contact_points() throws GeneralSecurityException, IOException {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
            "datastax-java-driver",
            "basic.contact-points",
            "[host1.com,host2.com]",
            "basic.default-port",
            1234);
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points"))
        .containsExactly("host1.com:1234", "host2.com:1234");
  }

  @Test
  void should_crate_valid_contact_points_legacy() throws GeneralSecurityException, IOException {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "hosts", "[host1.com,host2.com]", "port", 1234);
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(settings.getDriverConfig().getStringList("basic.contact-points"))
        .containsExactly("host1.com:1234", "host2.com:1234");
  }

  @Test
  void should_throw_exception_when_port_not_a_number() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "port", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid value for dsbulk.driver.port, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_local_connections_not_a_number() {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "pooling.local.connections", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.local.connections, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_remote_connections_not_a_number() {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "pooling.remote.connections", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.remote.connections, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_local_requests_not_a_number() {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "pooling.local.requests", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.local.requests, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_remote_requests_not_a_number() {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "pooling.remote.requests", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.remote.requests, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_fetch_size_not_a_number() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "query.fetchSize", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.fetchSize, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_read_timeout_not_a_duration() {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "socket.readTimeout", "\"I am not a duration\"");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid value for dsbulk.driver.socket.readTimeout")
        .hasMessageContaining("Could not parse time unit 'duration' (try ns, us, ms, s, m, h, d)");
  }

  @Test
  void should_throw_exception_when_max_retries_not_a_number() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "policy.maxRetries", "NotANumber");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.policy.maxRetries, expecting NUMBER, got STRING");
  }

  @Test
  void should_throw_exception_when_idempotence_not_a_boolean() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "query.idempotence", "NotABoolean");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.idempotence, expecting BOOLEAN, got STRING");
  }

  @Test
  void should_throw_exception_when_timestamp_generator_invalid() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "timestampGenerator", "Unknown");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.protocol.timestampGenerator, "
                + "expecting one of AtomicMonotonicTimestampGenerator, "
                + "ThreadLocalTimestampGenerator, ServerSideTimestampGenerator, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_address_translator_invalid() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "addressTranslator", "Unknown");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.protocol.addressTranslator, expecting IdentityTranslator, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_compression_invalid() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "protocol.compression", "Unknown");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.protocol.compression, expecting one of NONE, SNAPPY, LZ4, got 'Unknown'");
  }

  @Test
  void should_throw_exception_when_consistency_invalid() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "query.consistency", "Unknown");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.consistency, expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, got: 'Unknown'");
  }

  @Test
  void should_throw_exception_when_serial_consistency_invalid() {
    LoaderConfig oldConfig =
        createTestConfig("dsbulk.driver", "query.serialConsistency", "Unknown");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.query.serialConsistency, expecting one of ANY, ONE, TWO, THREE, QUORUM, ALL, LOCAL_ONE, LOCAL_QUORUM, EACH_QUORUM, SERIAL, LOCAL_SERIAL, got: 'Unknown'");
  }

  @Test
  void should_throw_exception_when_heartbeat_invalid() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "pooling.heartbeat", "NotADuration");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(true))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.driver.pooling.heartbeat: No number in duration value 'NotADuration'");
  }

  @ParameterizedTest
  @CsvSource({
    "port,9042,basic.contact-points",
    "hosts,[host.com],basic.contact-points",
    "protocol.compression,LZ4,advanced.protocol.compression",
    "pooling.local.connections,1234,advanced.connection.pool.local.size",
    "pooling.remote.connections,1234,advanced.connection.pool.remote.size",
    "pooling.local.requests,1234,advanced.connection.max-requests-per-connection",
    "pooling.remote.requests,1234,advanced.connection.max-requests-per-connection",
    "pooling.heartbeat,30 seconds,advanced.heartbeat.interval",
    "query.consistency,ONE,basic.request.consistency",
    "query.serialConsistency,SERIAL,basic.request.serial-consistency",
    "query.fetchSize,1000,basic.request.page-size",
    "query.idempotence,true,basic.request.default-idempotence",
    "socket.readTimeout,60 seconds,basic.request.timeout",
    "timestampGenerator,AtomicMonotonicTimestampGenerator,advanced.timestamp-generator.class",
    "addressTranslator,IdentityTranslator,advanced.address-translator.class",
    "policy.maxRetries,100,advanced.retry-policy.max-retries",
    "policy.lbp.dcAwareRoundRobin.localDc,testDC,basic.load-balancing-policy.local-datacenter",
    "policy.lbp.whiteList.hosts,[127.0.0.1],basic.load-balancing-policy.filter.class",
  })
  void should_log_warning_when_deprecated_driver_setting_present(
      String deprecatedSetting,
      String value,
      String newSetting,
      @LogCapture(level = WARN, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", deprecatedSetting, value);
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.driver.%s is deprecated and will be removed in a future release; "
                    + "please configure the driver directly using datastax-java-driver.%s instead.",
                deprecatedSetting, newSetting));
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig =
        createTestConfig("dsbulk.executor.continuousPaging", deprecatedSetting, value);
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(false);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.executor.continuousPaging.%s is deprecated and will be removed in a future release; "
                    + "please configure the driver directly using datastax-java-driver.%s instead.",
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", obsoleteSetting, value);
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Setting dsbulk.driver.%s has been removed and is not honored anymore; "
                    + "please remove it from your configuration. "
                    + "To configure the load balancing policy, use datastax-java-driver.basic.load-balancing-policy.* instead",
                obsoleteSetting));
  }

  @Test
  void should_throw_exception_when_page_unit_invalid() {
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig =
        createTestConfig("dsbulk.executor.continuousPaging", "pageUnit", "NotAPageUnit");
    LoaderConfig newConfig = createTestConfig("datastax-java-driver");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    assertThatThrownBy(() -> settings.init(false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid value for dsbulk.executor.continuousPaging.pageUnit, expecting one of BYTES, ROWS, got 'NotAPageUnit'");
  }

  @Test
  void should_log_info_when_cloud_and_implicit_contact_points(
      @LogCapture(level = INFO, value = DriverSettings.class) LogInterceptor logs)
      throws GeneralSecurityException, IOException {
    assumeFalse(PlatformUtils.isWindows());
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "hosts", "[host.com]");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
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
    LoaderConfig oldConfig = createTestConfig("dsbulk.driver", "ssl.provider", "JDK");
    LoaderConfig cpConfig = createTestConfig("dsbulk.executor.continuousPaging");
    LoaderConfig newConfig =
        createTestConfig(
            "datastax-java-driver", "basic.cloud.secure-connect-bundle", "/path/to/bundle");
    DriverSettings settings = new DriverSettings(oldConfig, cpConfig, newConfig);
    settings.init(true);
    assertThat(logs)
        .hasMessageContaining(
            "Setting dsbulk.driver.ssl.* is deprecated and will be removed in a future release; "
                + "please configure the driver directly using datastax-java-driver.advanced.ssl-engine-factory.* instead.")
        .hasMessageContaining(
            "Explicit SSL configuration provided together with a cloud secure connect bundle: "
                + "SSL settings will be ignored.");
    assertThat(settings.getDriverConfig().hasPath("advanced.ssl-engine-factory")).isFalse();
    assertThat(settings.sslHandlerFactory).isNull();
  }
}
