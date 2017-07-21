/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api;

import com.datastax.driver.core.Row;
import java.util.Map;
import org.reactivestreams.Publisher;

/** */
public interface Connector extends AutoCloseable {

  Publisher<Record> read();

  default void write(Publisher<Row> rows) {
    // TODO
    throw new UnsupportedOperationException();
  }

  default void init() {}

  default void close() throws Exception {}

  default void configure(Map<String, Object> settings) throws Exception {}
}
