/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.tests;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;

/** */
public final class MockConnector implements Connector {

  private static Connector delegate;

  public static void setDelegate(Connector delegate) {
    MockConnector.delegate = delegate;
  }

  @Override
  public Supplier<? extends Publisher<Record>> read() {
    return delegate.read();
  }

  @Override
  public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
    return delegate.readByResource();
  }

  @Override
  public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
    return delegate.write();
  }

  @Override
  public void configure(LoaderConfig settings, boolean read) throws BulkConfigurationException {
    delegate.configure(settings, read);
  }

  @Override
  public void init() throws Exception {
    delegate.init();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @Override
  public RecordMetadata getRecordMetadata() {
    return delegate.getRecordMetadata();
  }

  @Override
  public int estimatedResourceCount() {
    return delegate.estimatedResourceCount();
  }

  @Override
  public boolean isWriteToStandardOutput() {
    return delegate.isWriteToStandardOutput();
  }
}
