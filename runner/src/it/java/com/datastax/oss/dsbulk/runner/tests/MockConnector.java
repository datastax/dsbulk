/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.runner.tests;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.base.Functions;
import com.datastax.oss.dsbulk.connectors.api.Connector;
import com.datastax.oss.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.DefaultResource;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.connectors.api.Resource;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
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
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public int readConcurrency() {
            return 1;
          }

          @Override
          public int writeConcurrency() {
            return 1;
          }

          @Override
          public boolean supports(@NonNull ConnectorFeature feature) {
            return true;
          }

          @NonNull
          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cql) -> GenericType.STRING;
          }

          @NonNull
          @Override
          public Publisher<Resource> read() {
            return Flux.just(
                new DefaultResource(
                    RecordUtils.DEFAULT_RESOURCE,
                    Flux.just(records).map(RecordUtils::cloneRecord)));
          }

          @NonNull
          @Override
          public Function<Publisher<Record>, Publisher<Record>> write() {
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
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public int readConcurrency() {
            return -1;
          }

          @Override
          public int writeConcurrency() {
            return 1;
          }

          @Override
          public boolean supports(@NonNull ConnectorFeature feature) {
            return true;
          }

          @NonNull
          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cql) -> GenericType.STRING;
          }

          @NonNull
          @Override
          public Publisher<Resource> read() {
            return Flux::just;
          }

          @NonNull
          @Override
          public Function<Publisher<Record>, Publisher<Record>> write() {
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
          public void configure(
              @NonNull Config settings, boolean read, boolean retainRecordSources) {}

          @Override
          public int readConcurrency() {
            return -1;
          }

          @Override
          public int writeConcurrency() {
            return 1;
          }

          @Override
          public boolean supports(@NonNull ConnectorFeature feature) {
            return true;
          }

          @NonNull
          @Override
          public RecordMetadata getRecordMetadata() {
            return (field, cql) -> GenericType.STRING;
          }

          @NonNull
          @Override
          public Publisher<Resource> read() {
            return Flux::just;
          }

          @NonNull
          @Override
          public Function<Publisher<Record>, Publisher<Record>> write() {
            return upstream -> Flux.from(upstream).doOnNext(r -> records.incrementAndGet());
          }
        });
    return records;
  }

  @NonNull
  @Override
  public Publisher<Resource> read() {
    return delegate.read();
  }

  @NonNull
  @Override
  public Function<Publisher<Record>, Publisher<Record>> write() {
    return delegate.write();
  }

  @Override
  public void configure(@NonNull Config settings, boolean read, boolean retainRecordSources)
      throws IllegalArgumentException {
    delegate.configure(settings, read, retainRecordSources);
  }

  @Override
  public void init() throws Exception {
    delegate.init();
  }

  @Override
  public void close() throws Exception {
    delegate.close();
  }

  @NonNull
  @Override
  public RecordMetadata getRecordMetadata() {
    return delegate.getRecordMetadata();
  }

  @Override
  public boolean supports(@NonNull ConnectorFeature feature) {
    return delegate.supports(feature);
  }

  @Override
  public int readConcurrency() {
    return delegate.readConcurrency();
  }

  @Override
  public int writeConcurrency() {
    return delegate.writeConcurrency();
  }
}
