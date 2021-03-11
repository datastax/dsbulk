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
package com.datastax.oss.dsbulk.workflow.commons.settings;

public enum SchemaGenerationStrategy {
  MAP_AND_WRITE(true, false, true),
  READ_AND_MAP(true, false, false),
  READ_AND_COUNT(false, true, false);

  private final boolean mapping;
  private final boolean counting;
  private final boolean writing;

  SchemaGenerationStrategy(boolean mapping, boolean counting, boolean writing) {
    this.mapping = mapping;
    this.writing = writing;
    this.counting = counting;
  }

  /** @return true if this strategy requires a mapper, and false otherwise. */
  boolean isMapping() {
    return mapping;
  }

  /** @return true if this strategy requires a counter, and false otherwise. */
  boolean isCounting() {
    return counting;
  }

  /** @return true if this strategy requires writing data to the database, and false otherwise. */
  boolean isWriting() {
    return writing;
  }

  /** @return true if this strategy requires reading data from the database, and false otherwise. */
  boolean isReading() {
    return !writing;
  }
}
