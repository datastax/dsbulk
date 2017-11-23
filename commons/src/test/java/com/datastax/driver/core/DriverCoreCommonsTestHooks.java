/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.driver.core;

public class DriverCoreCommonsTestHooks {

  public static ColumnDefinitions.Definition newDefinition(
      String keyspace, String table, String name, DataType type) {
    return new ColumnDefinitions.Definition(keyspace, table, name, type);
  }

  public static ColumnDefinitions newColumnDefinitions(ColumnDefinitions.Definition... cols) {
    return new ColumnDefinitions(cols, CodecRegistry.DEFAULT_INSTANCE);
  }
}
