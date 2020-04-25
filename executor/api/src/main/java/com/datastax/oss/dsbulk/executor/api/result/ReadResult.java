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
package com.datastax.oss.dsbulk.executor.api.result;

import com.datastax.oss.driver.api.core.cql.Row;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents one of the many results of a read operation.
 *
 * <p>Each result encapsulates one {@link Row} returned by the execution of a {@link #getStatement()
 * read statement}.
 */
public interface ReadResult extends Result {

  /**
   * Returns the encapsulated {@link Row} object for this read result, if present.
   *
   * <p>The value is present if the execution succeeded, and absent otherwise.
   *
   * @return the encapsulated {@link Row} object for this read result, if present.
   */
  @NonNull
  Optional<Row> getRow();

  /**
   * If the result is a success, invoke the specified consumer with the returned {@link Row},
   * otherwise do nothing.
   *
   * @param consumer block to be executed if the result is a success
   * @throws NullPointerException if the result is a success and {@code consumer} is null
   */
  default void ifSuccess(@NonNull Consumer<? super Row> consumer) {
    getRow().ifPresent(consumer);
  }
}
