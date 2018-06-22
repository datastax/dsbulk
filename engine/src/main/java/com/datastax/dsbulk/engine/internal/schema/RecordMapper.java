/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface RecordMapper {

  @NonNull
  BatchableStatement<?> map(@NonNull Record record);
}
