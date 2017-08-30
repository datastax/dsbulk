/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine;

import com.datastax.dsbulk.commons.config.LoaderConfig;

public enum WorkflowType {
  LOAD {
    @Override
    public Workflow newWorkflow(LoaderConfig config) throws Exception {
      return new LoadWorkflow(config);
    }
  },

  UNLOAD {
    @Override
    public Workflow newWorkflow(LoaderConfig config) throws Exception {
      return new UnloadWorkflow(config);
    }
  };

  public abstract Workflow newWorkflow(LoaderConfig config) throws Exception;
}
