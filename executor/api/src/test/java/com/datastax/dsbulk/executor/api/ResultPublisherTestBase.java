/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.executor.api.result.Result;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public abstract class ResultPublisherTestBase<R extends Result> extends PublisherVerification<R> {

  protected static final List<Row> ROWS =
      IntStream.range(1, 100).boxed().map(i -> mock(Row.class)).collect(Collectors.toList());

  protected ResultPublisherTestBase() {
    super(new TestEnvironment());
  }

  protected static void setUpCluster(Session session) {
    Cluster cluster = mock(Cluster.class);
    when(session.getCluster()).thenReturn(cluster);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getQueryOptions()).thenReturn(new QueryOptions().setFetchSize(100));
  }
}
