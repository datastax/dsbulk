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
import com.datastax.dsbulk.connectors.json.JsonConnector;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import com.datastax.dsbulk.executor.reactor.batch.ReactorStatementBatcher;
import com.typesafe.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

public class BatchSettings {
  private static final Logger LOGGER = LoggerFactory.getLogger(BatchSettings.class);

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
  private static final String MAX_SIZE_IN_BYTES = "maxSizeInBytes";
  private static final String MAX_BATCH_STATEMENTS = "maxBatchStatements";
  private static final String BUFFER_SIZE = "bufferSize";

  private final LoaderConfig config;

  private BatchMode mode;
  private long maxSizeInBytes;
  private int maxBatchStatements;
  private int bufferSize;

  BatchSettings(LoaderConfig config) {
    this.config = config;
  }

  public void init() {
    try {
      mode = config.getEnum(BatchMode.class, MODE);
      Optional<Integer> maxBatchSizeOpt = loadOptionalConfig(MAX_BATCH_SIZE, config::getInt);
      Optional<Integer> maxBatchStatementsOpt = loadOptionalConfig(MAX_BATCH_STATEMENTS, config::getInt);
      Optional<Long> maxSizeInBytesOpt = loadOptionalConfig(MAX_SIZE_IN_BYTES, config::getLong);
      if (maxBatchSizeOpt.isPresent() && !maxBatchStatementsOpt.isPresent()) {
        maxBatchStatements = maxBatchSizeOpt.get();
        LOGGER.warn("the {} parameter is deprecated, use {} instead", MAX_BATCH_SIZE, MAX_BATCH_STATEMENTS);
      } else if (!maxBatchSizeOpt.isPresent() && maxBatchStatementsOpt.isPresent()) {
        maxBatchStatements = maxBatchStatementsOpt.get();
      }

      if (maxBatchSizeOpt.isPresent() && maxBatchStatementsOpt.isPresent()) {
        throw new BulkConfigurationException(
            String.format("You cannot specify both %s AND %s "
                    + "consider using %s, because %s is deprecated",
                MAX_BATCH_SIZE,
                MAX_BATCH_STATEMENTS,
                MAX_BATCH_STATEMENTS,
                MAX_BATCH_SIZE
            ));
      }

      if (maxSizeInBytesOpt.orElse(-1L) <= 0 && maxBatchStatements <= 0) {
        throw new BulkConfigurationException(
            String.format(
                "Value for batch.maxSizeInBytes (%d) OR buffer.maxBatchStatements (%d) must be positive. "
                    + "See settings.md for more information.",
                maxSizeInBytes, maxBatchStatements));
      }
      maxSizeInBytes = maxSizeInBytesOpt.orElse(StatementBatcher.DEFAULT_MAX_SIZE_BYTES);

      int bufferConfig = config.getInt(BUFFER_SIZE);
      bufferSize = bufferConfig > -1 ? bufferConfig : maxBatchStatements;

      if (bufferSize < maxBatchStatements) {
        throw new BulkConfigurationException(
            String.format(
                "Value for batch.bufferSize (%d) must be greater than or equal to "
                    + "buffer.maxBatchStatements OR buffer.maxBatchSize (%d). See settings.md for more information.",
                bufferSize, maxBatchStatements));
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "batch");
    }
  }

  private<T> Optional<T> loadOptionalConfig(String name, Function<String, T> supplier) {
    try {
      return Optional.of(supplier.apply(name));
    } catch (ConfigException.Missing ex) {
      LOGGER.trace("cannot load config: ", ex);
      return Optional.empty();
    }
  }

  public boolean isBatchingEnabled() {
    return mode != BatchMode.DISABLED;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public long getMaxSizeInBytes() {
    return maxSizeInBytes;
  }

  public int getMaxBatchStatements() {
    return maxBatchStatements;
  }

  public ReactorStatementBatcher newStatementBatcher(Cluster cluster) {
    return new ReactorStatementBatcher(
        cluster,
        mode.asStatementBatcherMode(),
        BatchStatement.Type.UNLOGGED,
        maxBatchStatements,
        maxSizeInBytes);
  }
}
