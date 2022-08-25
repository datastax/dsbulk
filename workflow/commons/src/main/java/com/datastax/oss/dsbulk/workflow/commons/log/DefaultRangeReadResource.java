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
package com.datastax.oss.dsbulk.workflow.commons.log;

import com.datastax.oss.dsbulk.executor.api.reader.BulkReader;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import com.datastax.oss.dsbulk.workflow.commons.statement.RangeReadBoundStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;
import org.reactivestreams.Publisher;

public class DefaultRangeReadResource implements RangeReadResource {

  private final RangeReadBoundStatement statement;
  private final BulkReader executor;

  public DefaultRangeReadResource(
      @NonNull RangeReadBoundStatement statement, @NonNull BulkReader executor) {
    this.statement = statement;
    this.executor = executor;
  }

  @NonNull
  @Override
  public URI getURI() {
    return statement.getResource();
  }

  @NonNull
  @Override
  public Publisher<ReadResult> read() {
    return executor.readReactive(statement);
  }
}
