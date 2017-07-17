/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api; /*
                                             * Copyright (C) 2017 DataStax Inc.
                                             *
                                             * This software can be used solely with DataStax Enterprise. Please consult the license at
                                             * http://www.datastax.com/terms/datastax-dse-driver-license-terms
                                             */

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import org.reactivestreams.Publisher;

/** */
public interface Connector extends AutoCloseable {

  Publisher<Statement> read();

  void write(Publisher<Row> rows);

  default void init() {}

  default void close() throws Exception {}
}
