/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.json;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.json.internal.SchemaFreeJsonRecordMetadata;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/** */
public class JsonConnector implements Connector {

  @Override
  public Publisher<Record> read() {
    // TODO
    return null;
  }

  @Override
  public Publisher<Publisher<Record>> readByResource() {
    // TODO
    return null;
  }

  @Override
  public Subscriber<Record> write() {
    // TODO
    return null;
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
  public void configure(LoaderConfig settings, boolean read) {}
}
