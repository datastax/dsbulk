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

  private enum BatchMode {
    DISABLED {
      @Override
      StatementBatcher.BatchMode asStatementBatcherMode() {
        throw new IllegalStateException("Batching is disabled");
      }
    },
    PARTITION_KEY {
      @Override
      StatementBatcher.BatchMode asStatementBatcherMode() {
        return StatementBatcher.BatchMode.PARTITION_KEY;
      }
    },
    REPLICA_SET {
      @Override
      StatementBatcher.BatchMode asStatementBatcherMode() {
        return StatementBatcher.BatchMode.REPLICA_SET;
      }
    };

    abstract StatementBatcher.BatchMode asStatementBatcherMode();
  }

  private static final String MODE = "mode";
  private static final String MAX_BATCH_SIZE = "maxBatchSize";
  private static final String BUFFER_SIZE = "bufferSize";

  private final LoaderConfig config;

  private BatchMode mode;
  private int maxBatchSize;
  private int bufferSize;

  BatchSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      mode = config.getEnum(BatchMode.class, MODE);
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
    return mode != BatchMode.DISABLED;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public ReactorStatementBatcher newStatementBatcher(Cluster cluster) {
    return new ReactorStatementBatcher(
        cluster, mode.asStatementBatcherMode(), BatchStatement.Type.UNLOGGED, maxBatchSize);
  }
}
