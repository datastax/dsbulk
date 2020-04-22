/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api;

import com.datastax.oss.dsbulk.executor.api.reader.ReactiveBulkReader;
import com.datastax.oss.dsbulk.executor.api.writer.ReactiveBulkWriter;

/**
 * An execution unit for {@link ReactiveBulkWriter bulk writes} and {@link ReactiveBulkReader bulk
 * reads} that operates in reactive mode.
 */
public interface ReactiveBulkExecutor extends ReactiveBulkWriter, ReactiveBulkReader {}
