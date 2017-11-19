/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.CCMRule;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.DSERequirement;
import java.util.List;
import javax.inject.Inject;
import org.assertj.core.api.Assertions;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.experimental.categories.Category;

@CCMConfig(numberOfNodes = 1)
@DSERequirement(min = "5.1")
@Category(LongTests.class)
public abstract class AbstractCCMEndToEndIT {

  @Rule @ClassRule public static CCMRule ccm = new CCMRule();

  @Inject private static CCMCluster ccmCluster;

  static void validateResultSetSize(int numOfQueries, String statement, Session session) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    Assertions.assertThat(results.size()).isEqualTo(numOfQueries);
  }

  static String[] addContactPointAndPort(List<String> args) {
    args.add("--driver.pooling.local.connections");
    args.add("1");
    args.add("--driver.hosts");
    args.add(ccmCluster.getInitialContactPoints().get(0).getHostAddress());
    args.add("--driver.port");
    args.add(Integer.toString(ccmCluster.getBinaryPort()));
    return args.toArray(new String[args.size()]);
  }
}
