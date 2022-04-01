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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.github.tomakehurst.wiremock.client.WireMock.aResponse;
import static com.github.tomakehurst.wiremock.client.WireMock.containing;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.notMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.awaitility.Awaitility.await;
import static org.slf4j.event.Level.ERROR;

import com.codahale.metrics.MetricRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.io.Resources;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.utils.NetworkUtils;
import com.datastax.oss.dsbulk.workflow.commons.metrics.prometheus.PrometheusManager.PullConfig;
import com.datastax.oss.dsbulk.workflow.commons.metrics.prometheus.PrometheusManager.PushConfig;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import ru.lanwen.wiremock.ext.WiremockResolver;
import ru.lanwen.wiremock.ext.WiremockResolver.Wiremock;

class PrometheusManagerTest {

  private MetricRegistry registry;

  @BeforeEach
  void setUp() {
    registry = new MetricRegistry();
  }

  @Test
  void should_expose_http_server_for_scraping() throws IOException {
    // given
    int port = NetworkUtils.findAvailablePort();
    PullConfig pullConfig = new PullConfig("", port);
    PrometheusManager manager =
        new PrometheusManager(
            registry, "execution1", "job1", ImmutableMap.of("name1", "value1"), pullConfig, null);
    URL url = new URL("http", "localhost", port, "/metrics");
    // when
    manager.init();
    try {
      manager.start();
      registry.counter("records/total").inc();
      // then
      List<String> data = Resources.readLines(url, StandardCharsets.UTF_8);
      assertThat(data)
          .anySatisfy(
              line ->
                  assertThat(line)
                      .startsWith("dsbulk_records_total")
                      .contains("application_version=")
                      .contains("client_id=")
                      .contains("driver_version=")
                      .contains("application_name=\"DataStax Bulk Loader execution1\"")
                      .contains("operation_id=\"execution1\"")
                      .contains("job=\"job1\"")
                      .contains("name1=\"value1\"")
                      .endsWith("1.0"));
    } finally {
      manager.close();
      await()
          .atMost(Duration.ofSeconds(5))
          .untilAsserted(
              () ->
                  assertThatThrownBy(() -> url.openConnection().connect())
                      .isInstanceOf(IOException.class));
    }
  }

  @Test
  void should_not_expose_http_server_when_wrong_config() {
    // given
    PullConfig pullConfig = new PullConfig("192.0.2.0", 0);
    PrometheusManager manager =
        new PrometheusManager(registry, "execution1", "job1", ImmutableMap.of(), pullConfig, null);
    // when
    manager.init();
    assertThatThrownBy(manager::start)
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining("ailed to start Prometheus Metrics HTTP server");
  }

  @ParameterizedTest
  @MethodSource
  @ExtendWith(WiremockResolver.class)
  void should_push_metrics_to_gateway(
      boolean success,
      boolean groupByInstance,
      boolean groupByOperationId,
      ImmutableMap<String, String> groupingKeys,
      String pathRegex,
      @Wiremock WireMockServer server)
      throws MalformedURLException {
    // given
    PushConfig pushConfig =
        new PushConfig(
            new URL(server.baseUrl()),
            "user1",
            "password1",
            groupByInstance,
            groupByOperationId,
            groupingKeys);
    PrometheusManager manager =
        new PrometheusManager(
            registry, "execution1", "job1", ImmutableMap.of("name1", "value1"), null, pushConfig);
    server.givenThat(post(urlPathEqualTo(pathRegex)).willReturn(aResponse().withStatus(201)));

    // when
    manager.init();
    registry.counter("records/total").inc(123456);
    manager.pushMetrics(Duration.ofSeconds(123), success);
    // then
    String expectedLabelsAsString =
        "{name1=\"value1\",application_version=\"1.8.1-SNAPSHOT\",application_name=\"DataStax Bulk Loader execution1\",client_id=\"de13b396-eb09-31d1-9876-cba56b790be0\",driver_version=\"4.14.0\",operation_id=\"execution1\",job=\"job1\",}";
    RequestPatternBuilder builder =
        postRequestedFor(urlPathMatching(pathRegex))
            .withHeader("Content-Type", containing("text/plain"))
            .withHeader("Authorization", equalTo("Basic dXNlcjE6cGFzc3dvcmQx"))
            .withRequestBody(
                containing("dsbulk_elapsed_time_seconds" + expectedLabelsAsString + " 123.0"))
            .withRequestBody(
                containing("dsbulk_records_total" + expectedLabelsAsString + " 123456.0"));

    if (success) {
      builder =
          builder
              .withRequestBody(containing("dsbulk_success" + expectedLabelsAsString + " 1.0"))
              .withRequestBody(containing("dsbulk_last_success" + expectedLabelsAsString));
    } else {
      builder =
          builder
              .withRequestBody(containing("dsbulk_success" + expectedLabelsAsString + " 0.0"))
              .withRequestBody(notMatching(".*dsbulk_last_success.*"));
    }
    server.verify(builder);
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_push_metrics_to_gateway() {
    return Stream.of(
        Arguments.of(true, false, false, ImmutableMap.of(), "/metrics/job/job1"),
        Arguments.of(false, false, false, ImmutableMap.of(), "/metrics/job/job1"),
        Arguments.of(true, true, false, ImmutableMap.of(), "/metrics/job/job1/instance/.+"),
        Arguments.of(
            true, false, true, ImmutableMap.of(), "/metrics/job/job1/operation_id/execution1"),
        Arguments.of(
            true,
            false,
            false,
            ImmutableMap.of("key1", "value1", "key2", "value2"),
            "/metrics/job/job1/key1/value1/key2/value2"),
        Arguments.of(
            true,
            true,
            true,
            ImmutableMap.of("key1", "value1", "key2", "value2"),
            "/metrics/job/job1/instance/.+/operation_id/execution1/key1/value1/key2/value2"));
  }

  @Test
  @ExtendWith(WiremockResolver.class)
  @ExtendWith(LogInterceptingExtension.class)
  void should_report_failed_gateway_push(
      @Wiremock WireMockServer server,
      @LogCapture(value = PrometheusManager.class, level = ERROR) LogInterceptor logs)
      throws MalformedURLException {
    // given
    PushConfig pushConfig =
        new PushConfig(new URL(server.baseUrl()), "", "", false, false, ImmutableMap.of());
    PrometheusManager manager =
        new PrometheusManager(registry, "execution1", "job1", ImmutableMap.of(), null, pushConfig);
    server.givenThat(
        post(urlPathEqualTo("/metrics/job/job1")).willReturn(aResponse().withStatus(503)));

    // when
    manager.init();
    manager.pushMetrics(Duration.ofSeconds(123), true);
    // then
    assertThat(logs)
        .hasMessageContaining(
            String.format(
                "Push to Prometheus PushGateway %s failed. Response code from %s/metrics/job/job1 was 503",
                server.baseUrl(), server.baseUrl()));
  }
}
