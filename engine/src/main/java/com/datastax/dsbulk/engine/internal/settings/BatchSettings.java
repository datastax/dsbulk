/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.dsbulk.executor.api.batch.ReactorUnsortedStatementBatcher;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import com.typesafe.config.ConfigException;

/** */
public class BatchSettings implements SettingsValidator {

  private final LoaderConfig config;

  BatchSettings(LoaderConfig config) {
    this.config = config;
  }

  public ReactorUnsortedStatementBatcher newStatementBatcher(Cluster cluster) {
    return new ReactorUnsortedStatementBatcher(
        cluster,
        config.getEnum(StatementBatcher.BatchMode.class, "mode"),
        config.getInt("maxBatchSize"),
        config.getInt("bufferSize"),
        config.getDuration("bufferTimeout"));
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getEnum(StatementBatcher.BatchMode.class, "mode");
      config.getInt("maxBatchSize");
      config.getInt("bufferSize");
      config.getDuration("bufferTimeout");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "batch");
    }
  }
}
