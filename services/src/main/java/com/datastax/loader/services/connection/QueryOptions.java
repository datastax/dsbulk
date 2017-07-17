/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.services.connection;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;

/** */
@SuppressWarnings("unused")
public class QueryOptions {

  private ConsistencyLevel consistencyLevel;

  private ConsistencyLevel serialConsistencyLevel;

  private int fetchSize;

  private boolean defaultIdempotence;

  public void configure(Cluster.Builder builder) {
    builder.withQueryOptions(
        new com.datastax.driver.core.QueryOptions()
            .setConsistencyLevel(consistencyLevel)
            .setSerialConsistencyLevel(serialConsistencyLevel)
            .setFetchSize(fetchSize)
            .setDefaultIdempotence(defaultIdempotence));
  }

  public ConsistencyLevel getConsistencyLevel() {
    return consistencyLevel;
  }

  public void setConsistencyLevel(ConsistencyLevel consistencyLevel) {
    this.consistencyLevel = consistencyLevel;
  }

  public ConsistencyLevel getSerialConsistencyLevel() {
    return serialConsistencyLevel;
  }

  public void setSerialConsistencyLevel(ConsistencyLevel serialConsistencyLevel) {
    this.serialConsistencyLevel = serialConsistencyLevel;
  }

  public int getFetchSize() {
    return fetchSize;
  }

  public void setFetchSize(int fetchSize) {
    this.fetchSize = fetchSize;
  }

  public boolean isDefaultIdempotence() {
    return defaultIdempotence;
  }

  public void setDefaultIdempotence(boolean defaultIdempotence) {
    this.defaultIdempotence = defaultIdempotence;
  }
}
