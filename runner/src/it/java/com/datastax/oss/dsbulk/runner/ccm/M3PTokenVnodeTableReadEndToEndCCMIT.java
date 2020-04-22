/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.runner.ccm;

import static com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig.UseKeyspaceMode.NONE;
import static org.slf4j.event.Level.INFO;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.oss.dsbulk.tests.driver.annotations.SessionConfig;
import com.datastax.oss.dsbulk.tests.logging.LogCapture;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import com.datastax.oss.dsbulk.tests.logging.StreamCapture;
import com.datastax.oss.dsbulk.tests.logging.StreamInterceptor;
import org.junit.jupiter.api.Tag;

@CCMConfig(numberOfNodes = 3, createOptions = "--vnodes")
@Tag("long")
class M3PTokenVnodeTableReadEndToEndCCMIT extends TableReadEndToEndCCMITBase {

  M3PTokenVnodeTableReadEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(useKeyspace = NONE) CqlSession session,
      @LogCapture(level = INFO) LogInterceptor interceptor,
      @StreamCapture StreamInterceptor stdout) {
    super(ccm, session, interceptor, stdout);
  }
}
