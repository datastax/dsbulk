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
package com.datastax.oss.dsbulk.sampler;

/**
 * A data container that can report the size of its contents.
 *
 * <p>This interface is used by {@link DataSizes} to determine the size of the data. It is meant to
 * be implemented by {@link com.datastax.oss.driver.api.core.cql.Row Row} and {@link
 * com.datastax.oss.driver.api.core.cql.Statement Statement} implementations wishing to optimize or
 * cache the data size computation.
 *
 * @see SizeableRow
 * @see SizeableBoundStatement
 * @see SizeableBatchStatement
 */
public interface Sizeable {

  /** @return the size of the container data in bytes. */
  long getDataSize();
}
