/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.policies;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.policies.RetryPolicy;

public class MultipleRetryPolicy implements RetryPolicy {
  private final int maxRetryCount;

  public MultipleRetryPolicy(int maxRetryCount) {
    this.maxRetryCount = maxRetryCount;
  }

  @Override
  public RetryDecision onReadTimeout(
      Statement statement,
      ConsistencyLevel cl,
      int requiredResponses,
      int receivedResponses,
      boolean dataRetrieved,
      int nbRetry) {
    return retryManyTimesOrThrow(cl, nbRetry);
  }

  @Override
  public RetryDecision onRequestError(
      Statement statement, ConsistencyLevel cl, DriverException e, int nbRetry) {
    return RetryDecision.rethrow();
  }

  @Override
  public RetryDecision onWriteTimeout(
      Statement statement,
      ConsistencyLevel cl,
      WriteType writeType,
      int requiredAcks,
      int receivedAcks,
      int nbRetry) {
    return retryManyTimesOrThrow(null, nbRetry);
  }

  @Override
  public RetryDecision onUnavailable(
      Statement statement,
      ConsistencyLevel cl,
      int requiredReplica,
      int aliveReplica,
      int nbRetry) {
    // We retry once in hope we connect to another
    // coordinator that can see more nodes (e.g. on another side of the network partition):
    return retryOnceOrThrow(nbRetry);
  }

  @Override
  public void init(Cluster cluster) {}

  @Override
  public void close() {}

  private RetryDecision retryManyTimesOrThrow(ConsistencyLevel cl, int nbRetry) {
    if (nbRetry < maxRetryCount) {
      return RetryDecision.retry(cl);
    } else {
      return RetryDecision.rethrow();
    }
  }

  private RetryDecision retryOnceOrThrow(int nbRetry) {
    if (nbRetry == 0) {
      return RetryDecision.retry(null);
    } else {
      return RetryDecision.rethrow();
    }
  }
}
