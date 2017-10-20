/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.settings;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.typesafe.config.ConfigException;

/** */
public class EngineSettings {

  private static final String MAX_CONCURRENT_MAPPINGS = "maxConcurrentMappings";
  private static final String BUFFER_SIZE = "bufferSize";
  private static final String DRY_RUN = "dryRun";

  private final int maxConcurrentMappings;
  private final int bufferSize;
  private final boolean dryRun;

  EngineSettings(LoaderConfig config) {
    try {
      maxConcurrentMappings = config.getThreads(MAX_CONCURRENT_MAPPINGS);
      bufferSize = config.getInt(BUFFER_SIZE);
      dryRun = config.getBoolean(DRY_RUN);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "engine");
    }
  }

  public int getMaxConcurrentMappings() {
    return maxConcurrentMappings;
  }

  public int getBufferSize() {
    return bufferSize;
  }

  public boolean isDryRun() {
    return dryRun;
  }
}
