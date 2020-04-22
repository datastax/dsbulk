/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.partitioner;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig.UseKeyspaceMode;
import org.junit.jupiter.api.Tag;

@CCMConfig(numberOfNodes = 3, createOptions = "--vnodes")
@Tag("long")
class M3PTokenVnodePartitionerCCMIT extends PartitionerCCMITBase {

  M3PTokenVnodePartitionerCCMIT(
      @SessionConfig(useKeyspace = UseKeyspaceMode.NONE) CqlSession session) {
    super(session, false);
  }
}
