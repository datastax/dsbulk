/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.executor.api.batch.ReactorStatementBatcher;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import com.typesafe.config.ConfigException;

/** */
public class BatchSettings {

  private static final String MODE = "mode";
  private static final String MAX_BATCH_SIZE = "maxBatchSize";
  private static final String BUFFER_SIZE = "bufferSize";
  private static final String ENABLED = "enabled";

  private final StatementBatcher.BatchMode mode;
  private final int maxBatchSize;
  private final int bufferSize;
  private final boolean enabled;

  BatchSettings(LoaderConfig config) {
    try {
      enabled = config.getBoolean(ENABLED);
      mode = config.getEnum(StatementBatcher.BatchMode.class, MODE);
      maxBatchSize = config.getInt(MAX_BATCH_SIZE);
      bufferSize = config.getInt(BUFFER_SIZE);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "batch");
    }
  }

  public boolean isBatchingEnabled() {
    return enabled;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public ReactorStatementBatcher newStatementBatcher(Cluster cluster) {
    return new ReactorStatementBatcher(cluster, mode, BatchStatement.Type.UNLOGGED, maxBatchSize);
  }
}
