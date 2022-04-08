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
package com.datastax.oss.dsbulk.workflow.commons.metrics.prometheus;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.workflow.api.utils.ThrowableUtils;
import com.datastax.oss.dsbulk.workflow.commons.settings.DriverSettings;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.dropwizard.DropwizardExports;
import io.prometheus.client.exporter.BasicAuthHttpConnectionFactory;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.exporter.PushGateway;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URL;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrometheusManager {

  public static class PullConfig {

    @NonNull private final String hostname;
    private final int port;

    public PullConfig(@NonNull String pullHostname, int pullPort) {
      this.hostname = pullHostname;
      this.port = pullPort;
    }
  }

  public static class PushConfig {

    @NonNull private final URL gatewayUrl;
    @NonNull private final String username;
    @NonNull private final String password;
    private final boolean groupByInstance;
    private final boolean groupByOperation;
    @NonNull private final ImmutableMap<String, String> groupByKeys;

    public PushConfig(
        @NonNull URL gatewayUrl,
        @NonNull String username,
        @NonNull String password,
        boolean groupByInstance,
        boolean groupByOperation,
        @NonNull ImmutableMap<String, String> groupByKeys) {
      this.gatewayUrl = gatewayUrl;
      this.username = username;
      this.password = password;
      this.groupByInstance = groupByInstance;
      this.groupByOperation = groupByOperation;
      this.groupByKeys = groupByKeys;
    }
  }

  /**
   * The metrics to push to PushGateway. We only push a few ones that are relevant after the
   * operation has finished.
   */
  private static final List<String> PUSH_GATEWAY_METRIC_NAMES =
      ImmutableList.of(
          "batches",
          "executor/bytes/received",
          "executor/bytes/sent",
          "executor/reads/failed",
          "executor/reads/successful",
          "executor/reads/total",
          "executor/writes/failed",
          "executor/writes/successful",
          "executor/writes/total",
          "records/failed",
          "records/total");

  private static final MetricFilter PUSH_METRIC_FILTER =
      (name, metric) -> PUSH_GATEWAY_METRIC_NAMES.contains(name);

  private static final String OPERATION_ID_LABEL = "operation_id";
  private static final String JOB_LABEL = "job";

  private static final Logger LOGGER = LoggerFactory.getLogger(PrometheusManager.class);

  @NonNull private final MetricRegistry registry;
  @NonNull private final String executionId;
  @Nullable private final PullConfig pullConfig;
  @Nullable private final PushConfig pushConfig;
  @NonNull private final String jobName;
  @NonNull private final ImmutableMap<String, String> labels;
  @NonNull private final BulkLoaderSampleBuilder sampleBuilder;

  private HTTPServer prometheusMetricsServer;

  public PrometheusManager(
      @NonNull MetricRegistry registry,
      @NonNull String executionId,
      @NonNull String jobName,
      @NonNull ImmutableMap<String, String> labelsFromConfig,
      @Nullable PullConfig pullConfig,
      @Nullable PushConfig pushConfig) {
    this.registry = registry;
    this.executionId = executionId;
    this.pullConfig = pullConfig;
    this.pushConfig = pushConfig;
    this.jobName = jobName;
    this.labels =
        ImmutableMap.<String, String>builder()
            .putAll(labelsFromConfig)
            .putAll(DriverSettings.driverPrometheusLabels(executionId))
            .put(OPERATION_ID_LABEL, executionId)
            .put(JOB_LABEL, jobName)
            .build();
    sampleBuilder = new BulkLoaderSampleBuilder(labels);
  }

  public void init() {
    if (pullConfig != null) {
      DefaultExports.initialize();
      new DropwizardExports(registry, sampleBuilder).register();
    }
  }

  public void start() {
    if (pullConfig != null) {
      try {
        prometheusMetricsServer =
            new HTTPServer.Builder()
                .withDaemonThreads(true)
                .withHostname(pullConfig.hostname.isEmpty() ? null : pullConfig.hostname)
                .withPort(pullConfig.port)
                .build();
      } catch (IOException e) {
        throw new UncheckedIOException("Failed to start Prometheus Metrics HTTP server", e);
      }
      LOGGER.info(
          "Prometheus Metrics HTTP server listening on {}:{}",
          pullConfig.hostname.isEmpty() ? "0.0.0.0" : pullConfig.hostname,
          pullConfig.port);
    }
  }

  public void close() {
    if (prometheusMetricsServer != null) {
      prometheusMetricsServer.close();
    }
  }

  public void pushMetrics(Duration elapsed, boolean success) {
    if (pushConfig != null) {
      try {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        new DropwizardExports(registry, PUSH_METRIC_FILTER, sampleBuilder)
            .register(collectorRegistry);
        addFinalMetrics(elapsed, success, collectorRegistry);
        PushGateway pg = new PushGateway(pushConfig.gatewayUrl);
        if (!pushConfig.username.isEmpty() && !pushConfig.password.isEmpty()) {
          pg.setConnectionFactory(
              new BasicAuthHttpConnectionFactory(pushConfig.username, pushConfig.password));
        }
        Map<String, String> groupingKeys = new LinkedHashMap<>();
        if (pushConfig.groupByInstance) {
          groupingKeys.putAll(PushGateway.instanceIPGroupingKey());
        }
        if (pushConfig.groupByOperation) {
          groupingKeys.put(OPERATION_ID_LABEL, executionId);
        }
        groupingKeys.putAll(pushConfig.groupByKeys);
        pg.pushAdd(collectorRegistry, jobName, groupingKeys);
      } catch (Exception e) {
        LOGGER.error(
            String.format(
                "Push to Prometheus PushGateway %s failed. %s",
                pushConfig.gatewayUrl, ThrowableUtils.getSanitizedErrorMessage(e)),
            e);
      }
    }
  }

  private void addFinalMetrics(
      Duration elapsed, boolean success, CollectorRegistry collectorRegistry) {
    String[] labelNames = labels.keySet().toArray(new String[0]);
    String[] labelValues = labels.values().toArray(new String[0]);
    Gauge.build()
        .name("dsbulk_elapsed_time_seconds")
        .help("Duration of DSBulk execution in seconds.")
        .labelNames(labelNames)
        .register(collectorRegistry)
        .labels(labelValues)
        .set(elapsed.getSeconds());
    Gauge.build()
        .name("dsbulk_success")
        .help("Whether DSBulk execution completed successfully.")
        .labelNames(labelNames)
        .register(collectorRegistry)
        .labels(labelValues)
        .set(success ? 1 : 0);
    if (success) {
      Gauge.build()
          .name("dsbulk_last_success")
          .help("Last time DSBulk completed successfully, in unixtime.")
          .labelNames(labelNames)
          .register(collectorRegistry)
          .labels(labelValues)
          .setToCurrentTime();
    } else {
      Gauge.build()
          .name("dsbulk_last_failure")
          .help("Last time DSBulk failed, in unixtime.")
          .labelNames(labelNames)
          .register(collectorRegistry)
          .labels(labelValues)
          .setToCurrentTime();
    }
  }
}
