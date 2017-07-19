/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.statement;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.loader.tests.ccm.CCMRule;
import com.datastax.loader.tests.ccm.annotations.CCMConfig;
import com.datastax.loader.tests.ccm.annotations.CCMTest;
import com.datastax.loader.tests.utils.CQLUtils;
import com.datastax.loader.tests.utils.StringUtils;
import java.util.List;
import javax.inject.Inject;
import org.junit.ClassRule;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

@CCMTest
@CCMConfig(numberOfNodes = 3)
public class TableScannerIT {

  @ClassRule
  public static CCMRule ccmRule = new CCMRule();

  @Inject
  public static Session session;

  @Test
  public void should_scan_table() throws Exception {
    String ks = StringUtils.uniqueIdentifier();
    session.execute(CQLUtils.createKeyspaceSimpleStrategy(ks, 1));
    session.execute(String.format("CREATE TABLE %s.t1 (pk int PRIMARY KEY, v int)", ks));
    Cluster cluster = session.getCluster();
    List<Statement> statements = TableScanner.scan(cluster, ks, "t1");
    assertThat(statements).hasSize(3);
    Metadata metadata = cluster.getMetadata();
    Statement stmt1 = statements.get(0);
    Statement stmt2 = statements.get(1);
    Statement stmt3 = statements.get(2);
    assertThat(stmt1.getRoutingToken()).isEqualTo(metadata.newToken("-3074457345618258603"));
    assertThat(stmt2.getRoutingToken()).isEqualTo(metadata.newToken("3074457345618258602"));
    assertThat(stmt3.getRoutingToken()).isEqualTo(metadata.newToken("-9223372036854775808"));
    assertThat(session.execute(stmt1).getExecutionInfo().getQueriedHost())
        .isEqualTo(metadata.getReplicas(ks, stmt1.getRoutingToken()).iterator().next());
    assertThat(session.execute(stmt2).getExecutionInfo().getQueriedHost())
        .isEqualTo(metadata.getReplicas(ks, stmt2.getRoutingToken()).iterator().next());
    assertThat(session.execute(stmt3).getExecutionInfo().getQueriedHost())
        .isEqualTo(metadata.getReplicas(ks, stmt3.getRoutingToken()).iterator().next());
  }
}
