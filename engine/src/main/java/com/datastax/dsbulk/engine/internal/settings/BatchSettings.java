/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import com.datastax.dsbulk.executor.reactor.batch.ReactorStatementBatcher;
import com.typesafe.config.ConfigException;

public class BatchSettings {

  private static final String MODE = "mode";
  private static final String MAX_BATCH_SIZE = "maxBatchSize";
  private static final String BUFFER_SIZE = "bufferSize";
  private static final String ENABLED = "enabled";

  private final LoaderConfig config;

  private StatementBatcher.BatchMode mode;
  private int maxBatchSize;
  private int bufferSize;
  private boolean enabled;

  BatchSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      enabled = config.getBoolean(ENABLED);
      mode = config.getEnum(StatementBatcher.BatchMode.class, MODE);
      maxBatchSize = config.getInt(MAX_BATCH_SIZE);
      int bufferConfig = config.getInt(BUFFER_SIZE);
      bufferSize = bufferConfig > -1 ? bufferConfig : maxBatchSize;
      if (bufferSize < maxBatchSize) {
        throw new BulkConfigurationException(
            String.format(
                "batch.bufferSize (%d) must be greater than or equal to buffer.maxBatchSize (%d). See settings.md for more information.",
                bufferSize, maxBatchSize));
      }
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
