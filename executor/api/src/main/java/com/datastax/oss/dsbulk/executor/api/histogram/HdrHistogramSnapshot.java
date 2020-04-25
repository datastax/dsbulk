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
package com.datastax.oss.dsbulk.executor.api.histogram;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.codahale.metrics.Snapshot;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.HistogramIterationValue;

/**
 * This is a slightly modified copy of the following code:
 * https://bitbucket.org/marshallpierce/hdrhistogram-metrics-reservoir, initially published under
 * COIL 0.5 (Copyfree) license.
 */
final class HdrHistogramSnapshot extends Snapshot {

  private final Histogram histogram;

  HdrHistogramSnapshot(Histogram histogram) {
    this.histogram = histogram;
  }

  @Override
  public double getValue(double quantile) {
    return histogram.getValueAtPercentile(quantile * 100.0);
  }

  @Override
  public long[] getValues() {
    long[] vals = new long[(int) histogram.getTotalCount()];
    int i = 0;

    for (HistogramIterationValue value : histogram.recordedValues()) {
      long val = value.getValueIteratedTo();

      for (int j = 0; j < value.getCountAddedInThisIterationStep(); j++) {
        vals[i] = val;

        i++;
      }
    }

    if (i != vals.length) {
      throw new IllegalStateException(
          "Total count was "
              + histogram.getTotalCount()
              + " but iterating values produced "
              + vals.length);
    }

    return vals;
  }

  @Override
  public int size() {
    return (int) histogram.getTotalCount();
  }

  @Override
  public long getMax() {
    return histogram.getMaxValue();
  }

  @Override
  public double getMean() {
    return histogram.getMean();
  }

  @Override
  public long getMin() {
    return histogram.getMinValue();
  }

  @Override
  public double getStdDev() {
    return histogram.getStdDeviation();
  }

  @Override
  public void dump(OutputStream output) {
    try (PrintWriter p = new PrintWriter(new OutputStreamWriter(output, UTF_8))) {
      for (HistogramIterationValue value : histogram.recordedValues()) {
        for (int j = 0; j < value.getCountAddedInThisIterationStep(); j++) {
          p.printf("%d%n", value.getValueIteratedTo());
        }
      }
    }
  }
}
