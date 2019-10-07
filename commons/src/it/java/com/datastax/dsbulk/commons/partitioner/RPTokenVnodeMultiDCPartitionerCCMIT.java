/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig.UseKeyspaceMode.NONE;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Tag;

@CCMConfig(
    numberOfNodes = {3, 3},
    createOptions = {"-p RandomPartitioner", "--vnodes"})
@Tag("long")
class RPTokenVnodeMultiDCPartitionerCCMIT extends PartitionerCCMITBase {

  RPTokenVnodeMultiDCPartitionerCCMIT(
      CCMCluster ccm, @SessionConfig(useKeyspace = NONE) CqlSession session) {
    super(ccm, session, true);
  }
}
