/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.typesafe.config.ConfigException;

/** */
public class EngineSettings implements SettingsValidator {

  private final LoaderConfig config;

  EngineSettings(LoaderConfig config) {
    this.config = config;
  }

  public int getMaxConcurrentMappings() {
    return config.getThreads("maxConcurrentMappings");
  }

  public int getBufferSize() {
    return config.getInt("bufferSize");
  }

  public boolean isDryRun() {
    return config.getBoolean("dryRun");
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getThreads("maxConcurrentMappings");
      config.getInt("bufferSize");
      config.getBoolean("dryRun");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "engine");
    }
  }
}
