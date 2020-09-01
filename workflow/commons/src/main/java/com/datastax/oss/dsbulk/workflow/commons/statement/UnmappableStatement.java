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

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.shaded.guava.common.base.MoreObjects;
import com.datastax.oss.dsbulk.connectors.api.Record;

public class UnmappableStatement extends MappedSimpleStatement {

  private final Throwable error;

  public UnmappableStatement(Record record, Throwable error) {
    super(record, SimpleStatement.newInstance(error.getMessage()));
    this.error = error;
  }

  public Throwable getError() {
    return error;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("record", getRecord())
        .add("error", error)
        .toString();
  }
}
