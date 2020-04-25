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
package com.datastax.oss.dsbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A {@link Record} that could not be properly processed.
 *
 * <p>It can be emitted in various situations, in particular:
 *
 * <ul>
 *   <li>When a {@link Connector} fails to create a record from data read from its external
 *       datasource (but subsequent reads are likely to succeed);
 *   <li>When a {@link Connector} fails to write a record to its external datasource (but subsequent
 *       writes are likely to succeed);
 *   <li>When a database {@linkplain com.datastax.oss.driver.api.core.cql.Row row} cannot be
 *       converted into a record.
 * </ul>
 */
public interface ErrorRecord extends Record {

  /**
   * Returns the error that prevented this record from being processed.
   *
   * @return the error that prevented this record from being processed.
   */
  @NonNull
  Throwable getError();
}
