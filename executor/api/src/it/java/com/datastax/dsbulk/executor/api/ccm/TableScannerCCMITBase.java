/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.ccm;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.utils.CQLUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.dsbulk.executor.api.statement.TableScanner;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class TableScannerCCMITBase {

  private final Session session;

  TableScannerCCMITBase(Session session) {
    this.session = session;
  }

  @ParameterizedTest(name = "[{index}] rf {0}")
  @ValueSource(ints = {1, 2, 3})
  void should_scan_table(int rf) {
    String ks = StringUtils.uniqueIdentifier("ks");
    session.execute(CQLUtils.createKeyspaceSimpleStrategy(ks, rf));
    session.execute(String.format("CREATE TABLE %s.t1 (pk int PRIMARY KEY, v int)", ks));
    for (int i = 0; i < 1000; i++) {
      session.execute(String.format("INSERT INTO %s.t1 (pk, v) VALUES (?, 1)", ks), i);
    }
    Cluster cluster = session.getCluster();
    Metadata metadata = cluster.getMetadata();
    List<TokenRange> unwrapped = new ArrayList<>();
    Set<TokenRange> ranges = metadata.getTokenRanges();
    for (TokenRange range : ranges) {
      if (range.isWrappedAround()) {
        unwrapped.addAll(range.unwrap());
      } else {
        unwrapped.add(range);
      }
    }
    List<Statement> statements = TableScanner.scan(cluster, ks, "t1");
    assertThat(statements).hasSize(unwrapped.size());
    int total = 0;
    for (int i = 0; i < unwrapped.size(); i++) {
      Statement stmt = statements.get(i);
      Token token = stmt.getRoutingToken();
      assertThat(token).isEqualTo(unwrapped.get(i).getEnd());
      ResultSet rs = session.execute(stmt);
      total += rs.all().size();
      assertThat(rs.getExecutionInfo().getQueriedHost()).isIn(metadata.getReplicas(ks, token));
    }
    assertThat(total).isEqualTo(1000);
  }
}
