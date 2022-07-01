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
package com.datastax.oss.dsbulk.workflow.commons.statement;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.dsbulk.partitioner.utils.TokenUtils;
import com.datastax.oss.dsbulk.workflow.commons.schema.ReadResultMapper;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.URI;

public interface RangeReadStatement {

  /**
   * Returns a "resource URI" identifying the operation's target table and token range. Suitable to
   * be used as the resource URI of records created by a {@link ReadResultMapper} targeting that
   * same table and token range.
   */
  @NonNull
  static URI rangeReadResource(
      @NonNull CqlIdentifier keyspace, @NonNull CqlIdentifier table, @NonNull TokenRange range) {
    // Contrary to other identifiers, keyspace and table names MUST contain only alpha-numeric
    // characters and underscores. So there is no need to escape path segments in the resulting URI.
    return URI.create(
        String.format(
            "cql://%s/%s?start=%s&end=%s",
            keyspace.asInternal(),
            table.asInternal(),
            TokenUtils.getTokenValue(range.getStart()),
            TokenUtils.getTokenValue(range.getEnd())));
  }

  @NonNull
  TokenRange getTokenRange();

  @NonNull
  URI getResource();
}
