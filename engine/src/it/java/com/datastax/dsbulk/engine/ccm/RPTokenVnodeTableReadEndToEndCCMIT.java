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

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.logging.StreamCapture;
import com.datastax.dsbulk.commons.tests.logging.StreamInterceptor;
import com.datastax.oss.driver.api.core.CqlSession;
import org.junit.jupiter.api.Tag;

@CCMConfig(
    numberOfNodes = 3,
    createOptions = {"-p RandomPartitioner", "--vnodes"})
@Tag("long")
class RPTokenVnodeTableReadEndToEndCCMIT extends TableReadEndToEndCCMITBase {

  RPTokenVnodeTableReadEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(useKeyspace = NONE) CqlSession session,
      @LogCapture(level = INFO) LogInterceptor interceptor,
      @StreamCapture StreamInterceptor stdout) {
    super(ccm, session, interceptor, stdout);
  }
}
