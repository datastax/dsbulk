/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.reactor;

import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.datastax.dsbulk.executor.reactor.writer.ReactorBulkWriter;

/**
 * An execution unit for {@link ReactorBulkWriter bulk writes} and {@link ReactorBulkReader bulk
 * reads} that operates in reactive mode using <a href="https://projectreactor.io">Reactor</a>.
 */
public interface ReactorBulkExecutor extends ReactorBulkWriter, ReactorBulkReader, BulkExecutor {}
