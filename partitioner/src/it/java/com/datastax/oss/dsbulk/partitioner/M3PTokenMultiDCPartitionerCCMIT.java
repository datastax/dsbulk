/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import static com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig.UseKeyspaceMode.NONE;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import org.junit.jupiter.api.Tag;

@CCMConfig(numberOfNodes = {3, 3})
@Tag("long")
class M3PTokenMultiDCPartitionerCCMIT extends PartitionerCCMITBase {

  M3PTokenMultiDCPartitionerCCMIT(@SessionConfig(useKeyspace = NONE) CqlSession session) {
    super(session, true);
  }
}
