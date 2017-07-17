/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api;

import com.datastax.loader.engine.api.reader.RxJavaBulkReader;
import com.datastax.loader.engine.api.writer.RxJavaBulkWriter;

/**
 * An execution unit for {@link RxJavaBulkWriter bulk writes} and {@link RxJavaBulkReader bulk
 * reads} that operates in reactive mode using <a
 * href="https://github.com/ReactiveX/RxJava/wiki">RxJava</a>.
 */
public interface RxJavaBulkExecutor extends RxJavaBulkWriter, RxJavaBulkReader, BulkExecutor {}
