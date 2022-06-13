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
package com.datastax.oss.dsbulk.runner.simulacron;

import static com.datastax.oss.driver.api.core.type.DataTypes.TEXT;
import static com.datastax.oss.dsbulk.runner.ExitStatus.STATUS_OK;
import static com.datastax.oss.dsbulk.runner.tests.EndToEndUtils.assertStatus;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.runner.DataStaxBulkLoader;
import com.datastax.oss.dsbulk.runner.ExitStatus;
import com.datastax.oss.dsbulk.runner.tests.MockConnector;
import com.datastax.oss.dsbulk.runner.tests.RecordUtils;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Column;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Keyspace;
import com.datastax.oss.dsbulk.tests.simulacron.SimulacronUtils.Table;
import com.datastax.oss.dsbulk.workflow.api.utils.WorkflowUtils;
import com.datastax.oss.simulacron.server.BoundCluster;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;
import reactor.core.publisher.Flux;

@Testcontainers
@Tag("medium")
class PrometheusEndToEndSimulacronIT extends EndToEndSimulacronITBase {

  @Container
  private final GenericContainer<?> prometheus =
      new GenericContainer<>(DockerImageName.parse("prom/prometheus:v2.33.3"))
          .withCopyFileToContainer(
              MountableFile.forClasspathResource("/metrics/prometheus.yml"),
              "/etc/prometheus/prometheus.yml")
          .withExposedPorts(9090)
          .withAccessToHost(true)
          .withNetwork(Network.SHARED);

  @Container
  private final GenericContainer<?> gateway =
      new GenericContainer<>(DockerImageName.parse("prom/pushgateway:v1.4.2"))
          .withExposedPorts(9091)
          .withNetwork(Network.SHARED)
          .withNetworkAliases("gateway");

  PrometheusEndToEndSimulacronIT(
      BoundCluster simulacron,
      @LogCapture(loggerName = "com.datastax.oss.dsbulk") LogInterceptor logs) {
    super(simulacron, logs, null, null);
  }

  @BeforeAll
  static void exposeHostPorts() {
    org.testcontainers.Testcontainers.exposeHostPorts(8080);
  }

  @Test
  void should_exchange_metrics_by_pull_and_push() throws IOException, InterruptedException {

    MockConnector.setDelegate(newConnectorDelegate());

    SimulacronUtils.primeTables(
        simulacron,
        new Keyspace(
            "ks1",
            new Table(
                "table1", new Column("pk", TEXT), new Column("cc", TEXT), new Column("v", TEXT))));

    String[] args = {
      "load",
      "-c",
      "mock",
      "--engine.executionId",
      "LOAD1",
      "--schema.keyspace",
      "ks1",
      "--schema.table",
      "table1",
      "--monitoring.console",
      "false",
      "--monitoring.prometheus.push.enabled",
      "true",
      "--monitoring.prometheus.pull.enabled",
      "true",
      "--monitoring.prometheus.labels",
      "{ foo = bar }",
      "--monitoring.prometheus.job",
      "job1",
      "--monitoring.prometheus.push.groupBy.instance",
      "true",
      "--monitoring.prometheus.push.url",
      "http://" + gateway.getHost() + ":" + gateway.getMappedPort(9091),
      "--monitoring.trackBytes",
      "true",
      "--driver.advanced.metrics.node.enabled",
      "pool.open-connections"
    };

    ExitStatus status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);

    assertThat(logs)
        .hasMessageContaining("Prometheus Metrics HTTP server listening on 0.0.0.0:8080");

    // Give some time for Prometheus to scrape the gateway
    Thread.sleep(7_000);

    // assert that metrics were pulled directly from DSBulk
    URL prometheusPullQuery =
        new URL(
            "http",
            "localhost",
            prometheus.getMappedPort(9090),
            "/api/v1/query?query=dsbulk_records_total"
                + "{instance=\"host.testcontainers.internal:8080\"}[5m]");
    assertThat(Resources.toString(prometheusPullQuery, StandardCharsets.UTF_8))
        .contains("\"status\":\"success\"")
        .contains("dsbulk_records_total")
        .contains("\"application_name\":\"DataStax Bulk Loader LOAD1\"")
        .contains("\"application_version\":\"" + WorkflowUtils.getBulkLoaderVersion() + "\"")
        .contains("\"client_id\":\"fc93e4ac-7fa5-394f-814e-21b735d04c10\"")
        .contains("\"driver_version\":\"" + Session.OSS_DRIVER_COORDINATES.getVersion() + "\"")
        .contains("\"instance\":\"host.testcontainers.internal:8080\"")
        .contains("\"job\":\"dsbulk\"")
        .contains("\"exported_job\":\"job1\"")
        .contains("\"foo\":\"bar\"")
        .contains("\"operation_id\":\"LOAD1\"");

    URL prometheusDriverPullQuery =
        new URL(
            "http",
            "localhost",
            prometheus.getMappedPort(9090),
            "/api/v1/query?query=dsbulk_driver_nodes_pool_open_connections"
                + "{instance=\"host.testcontainers.internal:8080\"}[5m]");
    assertThat(Resources.toString(prometheusDriverPullQuery, StandardCharsets.UTF_8))
        .contains("\"status\":\"success\"")
        .contains("dsbulk_driver_nodes_pool_open_connections")
        .contains("\"node\":\"" + hostname + ":" + port + "\"")
        .contains("\"application_name\":\"DataStax Bulk Loader LOAD1\"")
        .contains("\"application_version\":\"" + WorkflowUtils.getBulkLoaderVersion() + "\"")
        .contains("\"client_id\":\"fc93e4ac-7fa5-394f-814e-21b735d04c10\"")
        .contains("\"driver_version\":\"" + Session.OSS_DRIVER_COORDINATES.getVersion() + "\"")
        .contains("\"instance\":\"host.testcontainers.internal:8080\"")
        .contains("\"job\":\"dsbulk\"")
        .contains("\"exported_job\":\"job1\"")
        .contains("\"foo\":\"bar\"")
        .contains("\"operation_id\":\"LOAD1\"");

    // assert that metrics were pushed to the gateway
    URL gatewayQuery = new URL("http", "localhost", gateway.getMappedPort(9091), "/metrics");
    String labelsAndValue =
        "\\{"
            + "application_name=\"DataStax Bulk Loader LOAD1\","
            + "application_version=\""
            + WorkflowUtils.getBulkLoaderVersion()
            + "\","
            + "client_id=\"fc93e4ac-7fa5-394f-814e-21b735d04c10\","
            + "driver_version=\""
            + Session.OSS_DRIVER_COORDINATES.getVersion()
            + "\","
            + "foo=\"bar\","
            + "instance=\".+\","
            + "job=\"job1\","
            + "operation_id=\"LOAD1\"} .+";
    assertThat(Resources.readLines(gatewayQuery, StandardCharsets.UTF_8))
        .anySatisfy(line -> assertThat(line).matches("dsbulk_success" + labelsAndValue))
        .anySatisfy(line -> assertThat(line).matches("dsbulk_last_success" + labelsAndValue))
        .anySatisfy(line -> assertThat(line).matches("dsbulk_records_total" + labelsAndValue))
        // driver metrics not pushed to the gateway
        .allSatisfy(
            line -> assertThat(line).doesNotContain("dsbulk_driver_nodes_pool_open_connections"));

    // assert that Prometheus scraped DSBulk metrics from the gateway
    URL prometheusPullGatewayQuery =
        new URL(
            "http",
            "localhost",
            prometheus.getMappedPort(9090),
            "/api/v1/query?query=dsbulk_records_total{instance=\"gateway:9091\"}[5m]");
    assertThat(Resources.toString(prometheusPullGatewayQuery, StandardCharsets.UTF_8))
        .contains("\"status\":\"success\"")
        .contains("dsbulk_records_total")
        .contains("\"application_name\":\"DataStax Bulk Loader LOAD1\"")
        .contains("\"application_version\":\"" + WorkflowUtils.getBulkLoaderVersion() + "\"")
        .contains("\"client_id\":\"fc93e4ac-7fa5-394f-814e-21b735d04c10\"")
        .contains("\"driver_version\":\"" + Session.OSS_DRIVER_COORDINATES.getVersion() + "\"")
        .contains("\"instance\":\"gateway:9091\"")
        .contains("\"job\":\"gateway\"")
        .contains("\"exported_job\":\"job1\"")
        .contains("\"foo\":\"bar\"")
        .contains("\"operation_id\":\"LOAD1\"");

    // assert that driver metrics did not arrive to Prometheus through the gateway
    URL prometheusDriverPullGatewayQuery =
        new URL(
            "http",
            "localhost",
            prometheus.getMappedPort(9090),
            "/api/v1/query?query=dsbulk_driver_nodes_pool_open_connections{instance=\"gateway:9091\"}[5m]");
    assertThat(Resources.toString(prometheusDriverPullGatewayQuery, StandardCharsets.UTF_8))
        .isEqualTo("{\"status\":\"success\",\"data\":{\"resultType\":\"matrix\",\"result\":[]}}");
  }

  @NonNull
  private Connector newConnectorDelegate() {
    return new Connector() {

      @Override
      public int readConcurrency() {
        return 1;
      }

      @Override
      public boolean supports(@NonNull ConnectorFeature feature) {
        return feature == CommonConnectorFeature.INDEXED_RECORDS;
      }

      @NonNull
      @Override
      public Publisher<Publisher<Record>> read(
          BiFunction<URI, Publisher<Record>, Publisher<Record>> resourceTerminationHandler) {
        AtomicInteger counter = new AtomicInteger();
        AtomicBoolean running = new AtomicBoolean(true);
        URI resource = URI.create("file://file");
        return Flux.just(
            Flux.generate(
                sink -> {
                  int i = counter.incrementAndGet();
                  if (i == 1) {
                    startTimer(running);
                  }
                  if (running.get()) {
                    Record record =
                        RecordUtils.mappedCSV(
                            resource, i, "pk", "pk" + 1, "cc", "cc" + 1, "v", "v" + 1);
                    sink.next(record);
                  } else {
                    sink.complete();
                  }
                }));
      }

      @NonNull
      @Override
      public RecordMetadata getRecordMetadata() {
        return (fieldType, cqlType) -> GenericType.STRING;
      }

      @NonNull
      @Override
      public Function<Publisher<Record>, Publisher<Record>> write() {
        throw new UnsupportedOperationException();
      }

      @Override
      public int writeConcurrency() {
        throw new UnsupportedOperationException();
      }
    };
  }

  private void startTimer(AtomicBoolean running) {
    Timer timer = new Timer();
    timer.schedule(
        new TimerTask() {
          @Override
          public void run() {
            running.set(false);
          }
        },
        TimeUnit.SECONDS.toMillis(30));
  }
}
