/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.json;

import com.datastax.loader.commons.config.LoaderConfig;
import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.RecordMetadata;
import com.datastax.loader.connectors.json.internal.SchemaFreeJsonRecordMetadata;
import org.reactivestreams.Publisher;

/** */
public class JsonConnector implements Connector {

  @Override
  public Publisher<Record> read() {
    // TODO
    return null;
  }

  @Override
  public void write(Record record) {
    // TODO
  }

  @Override
  public void init() throws Exception {
    // TODO
  }

  @Override
  public void close() throws Exception {
    // TODO
  }

  @Override
  public RecordMetadata getRecordMetadata() {
    return new SchemaFreeJsonRecordMetadata();
  }

  @Override
  public void configure(LoaderConfig settings) {}
}
