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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import java.io.IOException;

public interface ReadResultCounter extends AutoCloseable {

  CountingUnit newCountingUnit(long initial);

  void reportTotals() throws IOException;

  interface CountingUnit extends AutoCloseable {

    void update(ReadResult result);
  }
}
