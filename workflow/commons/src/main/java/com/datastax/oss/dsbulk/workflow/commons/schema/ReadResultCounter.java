/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.schema;

import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import java.io.IOException;

public interface ReadResultCounter extends AutoCloseable {

  CountingUnit newCountingUnit();

  void reportTotals() throws IOException;

  interface CountingUnit extends AutoCloseable {

    void update(ReadResult result);
  }
}
