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

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.prometheus.client.Collector.MetricFamilySamples.Sample;
import io.prometheus.client.dropwizard.samplebuilder.DefaultSampleBuilder;
import java.util.ArrayList;
import java.util.List;

public class BulkLoaderSampleBuilder extends DefaultSampleBuilder {

  private static final String DSBULK_METRIC_PREFIX = "dsbulk.";

  private final ImmutableMap<String, String> labels;

  public BulkLoaderSampleBuilder(@NonNull ImmutableMap<String, String> labels) {
    this.labels = labels;
  }

  @Override
  public Sample createSample(
      String dropwizardName,
      String nameSuffix,
      List<String> additionalLabelNames,
      List<String> additionalLabelValues,
      double value) {
    List<String> labelNames = new ArrayList<>(additionalLabelNames);
    List<String> labelValues = new ArrayList<>(additionalLabelValues);
    labels.forEach(
        (n, v) -> {
          labelNames.add(n);
          labelValues.add(v);
        });
    dropwizardName = transformDriverNodeLevelMetric(dropwizardName, labelNames, labelValues);
    dropwizardName = DSBULK_METRIC_PREFIX + dropwizardName;
    return super.createSample(dropwizardName, nameSuffix, labelNames, labelValues, value);
  }

  /**
   * Detects if the metric name corresponds to a driver node-level metric; if so, transforms the
   * metric name and removes the node address from it and registers the detected node address as a
   * metric label instead.
   */
  @NonNull
  @VisibleForTesting
  static String transformDriverNodeLevelMetric(
      String dropwizardName, List<String> labelNames, List<String> labelValues) {
    int i = dropwizardName.indexOf(".nodes.");
    if (i != -1) {
      String suffix = dropwizardName.substring(i + 7);
      int j = suffix.indexOf(".");
      String node = suffix.substring(0, j);
      // The driver replaces dots with underscores, see DefaultEndPoint.buildMetricPrefix(); we
      // reinstate the dots here for a better visual result.
      node = node.replace('_', '.');
      labelNames.add("node");
      labelValues.add(node);
      dropwizardName = dropwizardName.substring(0, i + 6) + suffix.substring(j);
    }
    return dropwizardName;
  }
}
