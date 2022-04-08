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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class BulkLoaderSampleBuilderTest {

  @Test
  void should_create_sample() {
    ImmutableMap<String, String> labels = ImmutableMap.of("name1", "value1", "name2", "value2");
    BulkLoaderSampleBuilder builder = new BulkLoaderSampleBuilder(labels);
    Sample actual =
        builder.createSample("writes/total", "", new ArrayList<>(), new ArrayList<>(), 1.0);
    assertThat(actual.name).isEqualTo("dsbulk_writes_total");
    assertThat(actual.labelNames).containsExactly("name1", "name2");
    assertThat(actual.labelValues).containsExactly("value1", "value2");
    assertThat(actual.value).isEqualTo(1.0);
  }

  @Test
  void should_not_transform_session_level_metric() {
    List<String> labelNames = new ArrayList<>();
    List<String> labelValues = new ArrayList<>();
    String actual =
        BulkLoaderSampleBuilder.transformDriverNodeLevelMetric(
            "driver.cql-requests", labelNames, labelValues);
    assertThat(actual).isEqualTo("driver.cql-requests");
    assertThat(labelNames).isEmpty();
    assertThat(labelValues).isEmpty();
  }

  @Test
  void should_transform_node_level_metric() {
    List<String> labelNames = new ArrayList<>();
    List<String> labelValues = new ArrayList<>();
    String actual =
        BulkLoaderSampleBuilder.transformDriverNodeLevelMetric(
            "driver.nodes.host_com:9042.cql-messages", labelNames, labelValues);
    assertThat(actual).isEqualTo("driver.nodes.cql-messages");
    assertThat(labelNames).containsOnly("node");
    assertThat(labelValues).containsOnly("host.com:9042");
  }
}
