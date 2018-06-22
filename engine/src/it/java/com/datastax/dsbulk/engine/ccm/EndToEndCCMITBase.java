/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static java.nio.file.Files.createTempDirectory;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class EndToEndCCMITBase {

  final CCMCluster ccm;
  final CqlSession session;
  final ProtocolVersion protocolVersion;

  Path logDir;
  Path unloadDir;

  EndToEndCCMITBase(CCMCluster ccm, CqlSession session) {
    this.ccm = ccm;
    this.session = session;
    this.protocolVersion = session.getContext().getProtocolVersion();
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    LogUtils.resetLogbackConfiguration();
  }

  void validateResultSetSize(int numOfQueries, String statement) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    Assertions.assertThat(results.size()).isEqualTo(numOfQueries);
  }

  String[] addCommonSettings(List<String> args) {
    args.add("--log.directory");
    args.add(quoteJson(logDir));
    args.add("--driver.pooling.local.connections");
    args.add("1");
    args.add("--driver.hosts");
    args.add(ccm.getInitialContactPoints().get(0).getHostAddress());
    args.add("--driver.port");
    args.add(Integer.toString(ccm.getBinaryPort()));
    args.add("--driver.policy.lbp.localDc");
    args.add(ccm.getDC(1));
    return args.toArray(new String[0]);
  }
}
