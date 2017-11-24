/*
 * Copyright DataStax Inc.
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

  private static final String DRY_RUN = "dryRun";

  private final boolean dryRun;

  EngineSettings(LoaderConfig config) {
    try {
      dryRun = config.getBoolean(DRY_RUN);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "engine");
    }
  }

  public boolean isDryRun() {
    return dryRun;
  }
}
