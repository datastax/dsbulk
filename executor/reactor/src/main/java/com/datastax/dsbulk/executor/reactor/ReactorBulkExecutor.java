/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
