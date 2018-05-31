/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig.UseKeyspaceMode.NONE;
import static org.slf4j.event.Level.INFO;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;

@CCMConfig(
  numberOfNodes = 3,
  createOptions = {"-p RandomPartitioner", "--vnodes"}
)
class RPTokenVnodeTableReadEndToEndCCMITBase extends TableReadEndToEndCCMITBase {

  RPTokenVnodeTableReadEndToEndCCMITBase(
      CCMCluster ccm,
      @SessionConfig(useKeyspace = NONE) Session session,
      @LogCapture(level = INFO) LogInterceptor interceptor) {
    super(ccm, session, interceptor);
  }
}
