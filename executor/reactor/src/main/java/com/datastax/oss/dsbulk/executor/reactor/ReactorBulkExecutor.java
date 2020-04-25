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
package com.datastax.oss.dsbulk.executor.reactor;

import com.datastax.oss.dsbulk.executor.api.BulkExecutor;
import com.datastax.oss.dsbulk.executor.reactor.reader.ReactorBulkReader;
import com.datastax.oss.dsbulk.executor.reactor.writer.ReactorBulkWriter;

/**
 * An execution unit for {@link ReactorBulkWriter bulk writes} and {@link ReactorBulkReader bulk
 * reads} that operates in reactive mode using <a href="https://projectreactor.io">Reactor</a>.
 */
public interface ReactorBulkExecutor extends ReactorBulkWriter, ReactorBulkReader, BulkExecutor {}
