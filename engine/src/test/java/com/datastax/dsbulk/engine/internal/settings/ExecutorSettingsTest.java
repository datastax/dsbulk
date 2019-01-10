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
import static com.datastax.dsbulk.commons.tests.utils.ReflectionUtils.getInternalState;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.executor.api.reader.ReactiveBulkReader;
import com.datastax.dsbulk.executor.api.writer.ReactiveBulkWriter;
import com.datastax.dsbulk.executor.reactor.ContinuousReactorBulkExecutor;
import com.datastax.dsbulk.executor.reactor.DefaultReactorBulkExecutor;
import com.google.common.util.concurrent.RateLimiter;
import com.typesafe.config.ConfigFactory;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@SuppressWarnings({"unchecked", "UnstableApiUsage"})
class ExecutorSettingsTest {

  private Session session;
  private ContinuousPagingSession dseSession;
  private QueryOptions queryOptions;

  @BeforeEach
  void setUp() {
    session = mock(Session.class);
    dseSession = mock(ContinuousPagingSession.class);
    Cluster cluster = mock(Cluster.class);
    when(session.getCluster()).thenReturn(cluster);
    when(dseSession.getCluster()).thenReturn(cluster);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.DSE_V1);
    queryOptions = mock(QueryOptions.class);
    when(configuration.getQueryOptions()).thenReturn(queryOptions);
    when(queryOptions.getConsistencyLevel()).thenReturn(ConsistencyLevel.LOCAL_ONE);
  }

  @Test
  void should_create_non_continuous_executor_when_write_workflow() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkWriter executor = settings.newWriteExecutor(session, null);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_session_not_dse(
      @LogCapture LogInterceptor logs) {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
    assertThat(logs)
        .hasMessageContaining(
            "Continuous paging is not available, read performance will not be optimal");
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_wrong_CL(
      @LogCapture LogInterceptor logs) {
    when(queryOptions.getConsistencyLevel()).thenReturn(ConsistencyLevel.TWO);
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
    assertThat(logs)
        .hasMessageContaining(
            "Continuous paging is not available, read performance will not be optimal");
  }

  @Test
  void should_create_continuous_executor_when_read_workflow_and_session_dse() {
    LoaderConfig config =
        new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.executor"));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat(executor).isNotNull().isInstanceOf(ContinuousReactorBulkExecutor.class);
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_not_enabled() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("continuousPagingOptions.enabled = false")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, false);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
  }

  @Test
  void should_create_non_continuous_executor_when_read_workflow_and_search_query(
      @LogCapture LogInterceptor logs) {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("continuousPagingOptions.enabled = false")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(session, null, true);
    assertThat(executor).isNotNull().isInstanceOf(DefaultReactorBulkExecutor.class);
    assertThat(logs)
        .hasMessageContaining(
            "Continuous paging is enabled but is not compatible with search queries; disabling");
  }

  @Test
  void should_enable_maxPerSecond() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxPerSecond=100")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat((Optional<RateLimiter>) getInternalState(executor, "rateLimiter"))
        .isNotEmpty()
        .hasValueSatisfying(s -> assertThat(s.getRate()).isEqualTo(100));
  }

  @Test
  void should_disable_maxPerSecond() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxPerSecond=0")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat((Optional<RateLimiter>) getInternalState(executor, "rateLimiter")).isEmpty();
  }

  @Test
  void should_throw_exception_when_maxPerSecond_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxPerSecond=NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for executor.maxPerSecond: Expecting NUMBER, got STRING");
  }

  @Test
  void should_enable_maxInFlight() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxInFlight=100")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat((Optional<Semaphore>) getInternalState(executor, "requestPermits"))
        .isNotEmpty()
        .hasValueSatisfying(s -> assertThat(s.availablePermits()).isEqualTo(100));
  }

  @Test
  void should_disable_maxInFlight() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxInFlight=0")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat((Optional<Semaphore>) getInternalState(executor, "requestPermits")).isEmpty();
  }

  @Test
  void should_throw_exception_when_maxInFlight_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("maxInFlight=NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage("Invalid value for executor.maxInFlight: Expecting NUMBER, got STRING");
  }

  @Test
  void should_enable_maxConcurrentQueries() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("continuousPaging.maxConcurrentQueries=100")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat((Optional<Semaphore>) getInternalState(executor, "queryPermits"))
        .isNotEmpty()
        .hasValueSatisfying(s -> assertThat(s.availablePermits()).isEqualTo(100));
  }

  @Test
  void should_disable_maxConcurrentQueries() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("continuousPaging.maxConcurrentQueries=0")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    settings.init();
    ReactiveBulkReader executor = settings.newReadExecutor(dseSession, null, false);
    assertThat((Optional<Semaphore>) getInternalState(executor, "queryPermits")).isEmpty();
  }

  @Test
  void should_throw_exception_when_maxConcurrentQueries_not_a_number() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("continuousPaging.maxConcurrentQueries=NotANumber")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.executor")));
    ExecutorSettings settings = new ExecutorSettings(config);
    assertThatThrownBy(settings::init)
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid value for executor.continuousPaging.maxConcurrentQueries: Expecting NUMBER, got STRING");
  }
}
