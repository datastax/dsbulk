/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.metrics;

import com.codahale.metrics.Histogram;
import java.util.concurrent.Executors;
import org.junit.Test;

public class BatchReporterTest extends AbstractReporterTest {

  @Test
  public void should_report_batches() throws Exception {
    Histogram size = registry.histogram("batches/size");
    BatchReporter reporter =
        new BatchReporter(registry, Executors.newSingleThreadScheduledExecutor());
    reporter.report();
    verifyEventLogged("Batches: total: 0, size: 0.00 mean, 0 min, 0 max");
    size.update(2);
    size.update(1);
    size.update(1);
    size.update(1);
    reporter.report();
    verifyEventLogged("Batches: total: 4, size: 1.25 mean, 1 min, 2 max");
  }

  @Override
  Class<?> getLoggerClass() {
    return BatchReporter.class;
  }
}
