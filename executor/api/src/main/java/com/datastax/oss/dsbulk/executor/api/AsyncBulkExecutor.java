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
package com.datastax.oss.dsbulk.executor.api;

import com.datastax.oss.dsbulk.executor.api.reader.AsyncBulkReader;
import com.datastax.oss.dsbulk.executor.api.writer.AsyncBulkWriter;

/**
 * An execution unit for {@link AsyncBulkWriter bulk writes} and {@link AsyncBulkReader bulk reads}
 * that operates in asynchronous mode.
 */
public interface AsyncBulkExecutor extends AsyncBulkWriter, AsyncBulkReader {}
