/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

/** A Connector is a component capable of reading from and writing to a datasource. */
public interface Connector extends AutoCloseable {

  /**
   * Reads records from the datasource.
   *
   * @return a {@link Publisher} of records read from the datasource.
   */
  Publisher<Record> read();

  /**
   * Writes records to the datasource.
   *
   * @return A {@link Subscriber} of records to write to the datasource.
   */
  Subscriber<Record> write();

  /**
   * Configures the connector.
   *
   * @param settings the connector settings.
   * @param read whether the connector should be configured for reading or writing.
   * @throws BulkConfigurationException if the connector fails to configure properly.
   */
  default void configure(LoaderConfig settings, boolean read) throws BulkConfigurationException {}

  /**
   * Initializes the connector.
   *
   * @throws Exception if the connector fails to initialize properly.
   */
  default void init() throws Exception {}

  /**
   * Closes the connector.
   *
   * @throws Exception if the connector fails to close properly.
   */
  default void close() throws Exception {}

  /**
   * Returns metadata about the records that this connector can read or write.
   *
   * <p>This method should only be called after {@link #configure(LoaderConfig, boolean)} and {@link
   * #init()}, i.e., when the connector is fully initialized and ready to read or write.
   *
   * <p>If this connector cannot gather metadata, or if the metadata is inaccurate, then it should
   * signal this situation by returning {@link RecordMetadata#DEFAULT}.
   *
   * @return the metadata about the records that this connector can read or write.
   */
  default RecordMetadata getRecordMetadata() {
    return RecordMetadata.DEFAULT;
  }
}
