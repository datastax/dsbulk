/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.truncateIpByCountryTable;

import ch.qos.logback.core.joran.spi.JoranException;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.engine.tests.utils.EndToEndUtils;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.CCMExtension;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class EndToEndCCMITBase {

  private final CCMCluster ccm;
  final Session session;

  protected EndToEndCCMITBase(CCMCluster ccm, Session session) {
    this.ccm = ccm;
    this.session = session;
  }

  @AfterEach
  void truncateTable() {
    truncateIpByCountryTable(session);
  }

  @AfterEach
  void resetLogbackConfiguration() throws JoranException {
    EndToEndUtils.resetLogbackConfiguration();
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
    return args.toArray(new String[args.size()]);
  }
}
