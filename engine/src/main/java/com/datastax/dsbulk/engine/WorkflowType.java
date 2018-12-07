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
    public String getTitle() {
      return "load";
    }

    @Override
    public String getDescription() {
      return "Loads data from external data sources into DataStax Enterprise databases. "
          + "This command requires a connector to read data from; "
          + "the target table, or alternatively, the insert query must also be properly configured. "
          + "Run `dsbulk help connector` or `dsbulk help schema` for more information.";
    }

    @Override
    public Workflow newWorkflow(LoaderConfig config) {
      return new LoadWorkflow(config);
    }
  },

  UNLOAD {
    @Override
    public String getTitle() {
      return "unload";
    }

    @Override
    public String getDescription() {
      return "Unloads data from DataStax Enterprise or "
          + "Apache Cassandra (R) databases into external data sinks. "
          + "This command requires a connector to write data to; "
          + "the source table, or alternatively, the read query must also be properly configured. "
          + "Run `dsbulk help connector` or `dsbulk help schema` for more information.";
    }

    @Override
    public Workflow newWorkflow(LoaderConfig config) {
      return new UnloadWorkflow(config);
    }
  },

  COUNT {
    @Override
    public String getTitle() {
      return "count";
    }

    @Override
    public String getDescription() {
      return "Computes statistics about a table, such as "
          + "the total number of rows, "
          + "the number of rows per token range, "
          + "the number of rows per host, "
          + "or the total number of rows in the N biggest partitions in the table. "
          + "Run `dsbulk help stats` for more information.";
    }

    @Override
    public Workflow newWorkflow(LoaderConfig config) {
      return new CountWorkflow(config);
    }
  },
  ;

  public abstract String getTitle();

  public abstract String getDescription();

  public abstract Workflow newWorkflow(LoaderConfig config);
}
