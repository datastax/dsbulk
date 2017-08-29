/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.loader.commons.config.LoaderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** */
public class EngineSettings {

  private static final Logger LOGGER = LoggerFactory.getLogger(EngineSettings.class);

  private final LoaderConfig config;

  EngineSettings(LoaderConfig config) {
    this.config = config;
  }

  public int getMaxMappingThreads() {
    return config.getThreads("maxMappingThreads");
  }

  public int getMaxConcurrentReads() {
    return config.getThreads("maxConcurrentReads");
  }
}
