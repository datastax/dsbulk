/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.engine.tests.utils.LogUtils;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class EndToEndCCMITBase {

  protected final CCMCluster ccm;
  final Session session;
  final ProtocolVersion protocolVersion;

  EndToEndCCMITBase(CCMCluster ccm, Session session) {
    this.ccm = ccm;
    this.session = session;
    this.protocolVersion =
        session.getCluster().getConfiguration().getProtocolOptions().getProtocolVersion();
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

  String[] addContactPointAndPort(List<String> args) {
    args.add("--driver.pooling.local.connections");
    args.add("1");
    args.add("--driver.hosts");
    args.add(ccm.getInitialContactPoints().get(0).getHostAddress());
    args.add("--driver.port");
    args.add(Integer.toString(ccm.getBinaryPort()));
    return args.toArray(new String[0]);
  }

  public static String createURLFile(URL... urls) throws IOException {
    File file = File.createTempFile("urlfile", null);
    Files.write(
        file.toPath(),
        Arrays.stream(urls).map(URL::toExternalForm).collect(Collectors.toList()),
        Charset.forName("UTF-8"));
    return file.getAbsolutePath();
  }
}
