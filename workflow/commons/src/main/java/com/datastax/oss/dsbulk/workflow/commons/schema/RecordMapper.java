/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.schema;

import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.dsbulk.connectors.api.Record;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface RecordMapper {

  @NonNull
  BatchableStatement<?> map(@NonNull Record record);
}
