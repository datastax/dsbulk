/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine;

import com.datastax.loader.commons.config.LoaderConfig;

public enum WorkflowType {
  WRITE {
    @Override
    public Workflow newWorkflow(LoaderConfig config) throws Exception {
      return new WriteWorkflow(config);
    }
  },

  READ {
    @Override
    public Workflow newWorkflow(LoaderConfig config) throws Exception {
      return new ReadWorkflow(config);
    }
  };

  public abstract Workflow newWorkflow(LoaderConfig config) throws Exception;
}
