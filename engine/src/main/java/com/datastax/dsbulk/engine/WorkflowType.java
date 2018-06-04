/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine;

import com.datastax.dsbulk.commons.config.LoaderConfig;

public enum WorkflowType {
  LOAD {
    @Override
    public Workflow newWorkflow(LoaderConfig config) {
      return new LoadWorkflow(config);
    }
  },

  UNLOAD {
    @Override
    public Workflow newWorkflow(LoaderConfig config) {
      return new UnloadWorkflow(config);
    }
  },

  COUNT {
    @Override
    public Workflow newWorkflow(LoaderConfig config) {
      return new CountWorkflow(config);
    }
  },
  ;

  public abstract Workflow newWorkflow(LoaderConfig config);
}
