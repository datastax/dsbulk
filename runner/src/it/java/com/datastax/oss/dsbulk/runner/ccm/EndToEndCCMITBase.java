/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.runner.ccm;

import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMExtension;
import com.datastax.oss.dsbulk.tests.utils.FileUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Path;
import java.util.List;
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
  private final InetSocketAddress contactPoint;

  Path logDir;
  Path unloadDir;

  EndToEndCCMITBase(CCMCluster ccm, CqlSession session) {
    this.ccm = ccm;
    this.session = session;
    this.protocolVersion = session.getContext().getProtocolVersion();
    contactPoint = (InetSocketAddress) ccm.getInitialContactPoints().get(0).resolve();
  }

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    FileUtils.deleteDirectory(logDir);
    FileUtils.deleteDirectory(unloadDir);
  }

  void validateResultSetSize(int numOfQueries, String statement) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    assertThat(results.size()).isEqualTo(numOfQueries);
  }

  String[] addCommonSettings(List<String> args) {
    args.add("--log.directory");
    args.add(StringUtils.quoteJson(logDir));
    args.add("--driver.advanced.connection.pool.local.size");
    args.add("1");
    args.add("-h");
    args.add(StringUtils.quoteJson(contactPoint.getHostName() + ':' + contactPoint.getPort()));
    return args.toArray(new String[0]);
  }
}
