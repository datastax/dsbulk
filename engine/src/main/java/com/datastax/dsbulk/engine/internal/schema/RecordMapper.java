/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.connectors.api.Record;
import org.jetbrains.annotations.NotNull;

public interface RecordMapper {

  @NotNull
  Statement map(@NotNull Record record);
}
