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
import com.datastax.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.google.common.base.Functions;
import com.google.common.reflect.TypeToken;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/** A mock connector to help creating end-to-end integration tests. */
public final class MockConnector implements Connector {

  private static Connector delegate;

  public static void setDelegate(Connector delegate) {
    MockConnector.delegate = delegate;
  }

  /**
   * Sets up the mock connector to emulate reads; it will emit all the given records as if they were
   * read from an external source.
   *
   * @param records the records to emit.
   */
  public static void mockReads(Record... records) {
    setDelegate(
        new Connector() {

          @Override
          public void init() {}

          @Override
          public void configure(LoaderConfig settings, boolean read) {}

          @Override
          public int estimatedResourceCount() {
            return -1;
          }

          @Override
          public boolean supports(ConnectorFeature feature) {
            return true;
          }

          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cql) -> TypeToken.of(String.class);
          }

          @Override
          public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
            return () -> Flux.just(read().get());
          }

          @Override
          public Supplier<? extends Publisher<Record>> read() {
            return () -> Flux.just(records);
          }

          @Override
          public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
            return Functions.identity();
          }
        });
  }

  /**
   * Sets up the mock connector to emulate writes; it will store all received records as if they
   * were written to an external sink. The "written" records will appear in the returned list.
   *
   * @return the list where "written" records will be stored.
   */
  public static List<Record> mockWrites() {
    List<Record> records = new ArrayList<>();
    setDelegate(
        new Connector() {

          @Override
          public void init() {}

          @Override
          public void configure(LoaderConfig settings, boolean read) {}

          @Override
          public int estimatedResourceCount() {
            return -1;
          }

          @Override
          public boolean supports(ConnectorFeature feature) {
            return true;
          }

          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cql) -> TypeToken.of(String.class);
          }

          @Override
          public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
            return Flux::just;
          }

          @Override
          public Supplier<? extends Publisher<Record>> read() {
            return Flux::just;
          }

          @Override
          public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
            return upstream -> Flux.from(upstream).doOnNext(records::add);
          }
        });
    return records;
  }

  /**
   * Sets up the mock connector to emulate writes; it will acknowledge records as if they were
   * written to an external sink. The "written" records will be counted and the total number of
   * records "written" will be reflected in the returned AtomicInteger.
   *
   * @return a counter for the number of records "written".
   */
  public static AtomicInteger mockCountingWrites() {
    AtomicInteger records = new AtomicInteger();
    setDelegate(
        new Connector() {

          @Override
          public void init() {}

          @Override
          public void configure(LoaderConfig settings, boolean read) {}

          @Override
          public int estimatedResourceCount() {
            return -1;
          }

          @Override
          public boolean supports(ConnectorFeature feature) {
            return true;
          }

          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cql) -> TypeToken.of(String.class);
          }

          @Override
          public Supplier<? extends Publisher<Publisher<Record>>> readByResource() {
            return Flux::just;
          }

          @Override
          public Supplier<? extends Publisher<Record>> read() {
            return Flux::just;
          }

          @Override
          public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
            return upstream -> Flux.from(upstream).doOnNext(r -> records.incrementAndGet());
          }
        });
    return records;
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
  public boolean supports(ConnectorFeature feature) {
    return delegate.supports(feature);
  }

  @Override
  public int estimatedResourceCount() {
    return delegate.estimatedResourceCount();
  }
}
