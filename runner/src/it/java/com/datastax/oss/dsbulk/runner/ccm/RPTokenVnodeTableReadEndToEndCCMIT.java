/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
