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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class EngineSettings implements SettingsValidator {

  private static final Logger LOGGER = LoggerFactory.getLogger(EngineSettings.class);

  private final LoaderConfig config;

  EngineSettings(LoaderConfig config) {
    this.config = config;
  }

  public int getMaxMappingThreads() {
    return config.getThreads("maxMappingThreads");
  }

  public int getMaxConcurrentOps() {
    return config.getThreads("maxConcurrentOps");
  }

  public void validateConfig(WorkflowType type) throws BulkConfigurationException {
    try {
      config.getThreads("maxMappingThreads");
      config.getThreads("maxConcurrentOps");
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "engine");
    }
  }
}
