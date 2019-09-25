/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.policies.retry;

import com.datastax.dsbulk.engine.internal.settings.BulkDriverOption;
import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.connection.ClosedConnectionException;
import com.datastax.oss.driver.api.core.connection.HeartbeatException;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.retry.RetryDecision;
import com.datastax.oss.driver.api.core.retry.RetryPolicy;
import com.datastax.oss.driver.api.core.servererrors.CoordinatorException;
import com.datastax.oss.driver.api.core.servererrors.ReadFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteFailureException;
import com.datastax.oss.driver.api.core.servererrors.WriteType;
import com.datastax.oss.driver.api.core.session.Request;
import edu.umd.cs.findbugs.annotations.NonNull;

public class MultipleRetryPolicy implements RetryPolicy {

  private final int maxRetryCount;

  public MultipleRetryPolicy(DriverContext context, String profileName) {
    this.maxRetryCount =
        context
            .getConfig()
            .getProfile(profileName)
            .getInt(BulkDriverOption.RETRY_POLICY_MAX_RETRIES, 10);
  }

  @Override
  public RetryDecision onReadTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int blockFor,
      int received,
      boolean dataPresent,
      int retryCount) {
    return retryCount < maxRetryCount ? RetryDecision.RETRY_SAME : RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onWriteTimeout(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      @NonNull WriteType writeType,
      int blockFor,
      int received,
      int retryCount) {
    return retryCount < maxRetryCount ? RetryDecision.RETRY_SAME : RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onUnavailable(
      @NonNull Request request,
      @NonNull ConsistencyLevel cl,
      int required,
      int alive,
      int retryCount) {
    return retryCount < maxRetryCount ? RetryDecision.RETRY_NEXT : RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onRequestAborted(
      @NonNull Request request, @NonNull Throwable error, int retryCount) {
    return (error instanceof ClosedConnectionException || error instanceof HeartbeatException)
        ? RetryDecision.RETRY_NEXT
        : RetryDecision.RETHROW;
  }

  @Override
  public RetryDecision onErrorResponse(
      @NonNull Request request, @NonNull CoordinatorException error, int retryCount) {
    return (error instanceof ReadFailureException || error instanceof WriteFailureException)
        ? RetryDecision.RETHROW
        : RetryDecision.RETRY_NEXT;
  }

  @Override
  public void close() {}
}
