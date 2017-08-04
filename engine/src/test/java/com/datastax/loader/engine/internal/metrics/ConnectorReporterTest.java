/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.metrics;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.codahale.metrics.Meter;
import java.util.concurrent.Executors;
import org.junit.Test;

public class ConnectorReporterTest extends AbstractReporterTest {

  @Test
  public void should_report_batches() throws Exception {
    Meter totalMeter = registry.meter("records/total");
    Meter successfulMeter = registry.meter("records/successful");
    Meter failedMeter = registry.meter("records/failed");
    RecordReporter reporter =
        new RecordReporter(registry, SECONDS, Executors.newSingleThreadScheduledExecutor(), -1);
    reporter.report();
    verifyEventLogged("Records: total: 0, successful: 0, failed: 0, mean: 0 records/second");
    totalMeter.mark(3);
    successfulMeter.mark(2);
    failedMeter.mark(1);
    reporter.report();
    // can't assert mean rate as it may vary
    verifyEventLogged("Records: total: 3, successful: 2, failed: 1");
  }

  @Override
  Class<?> getLoggerClass() {
    return RecordReporter.class;
  }
}
