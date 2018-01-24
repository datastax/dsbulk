/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.histogram;

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
