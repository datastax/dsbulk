/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.driver.core.ConsistencyLevel.ALL;
import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.utils.CQLUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import java.util.List;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class PartitionerCCMITBase {

  private final Session session;
  private final boolean multiDc;

  PartitionerCCMITBase(Session session, boolean multiDc) {
    this.session = session;
    this.multiDc = multiDc;
  }

  @ParameterizedTest(name = "[{index}] rf {0}")
  @ValueSource(ints = {1, 2, 3})
  void should_scan_table(int rf) {
    String ks = StringUtils.uniqueIdentifier("MY_KS");
    if (multiDc) {
      session.execute(CQLUtils.createKeyspaceNetworkTopologyStrategy(ks, rf, rf));
    } else {
      session.execute(CQLUtils.createKeyspaceSimpleStrategy(ks, rf));
    }
    ks = Metadata.quote(ks);
    session.execute(
        String.format("CREATE TABLE %s.\"MY_TABLE\" (\"PK\" int PRIMARY KEY, \"V\" int)", ks));
    int expectedTotal = 10000;
    for (int i = 0; i < expectedTotal; i++) {
      Statement stmt =
          new SimpleStatement(
                  String.format("INSERT INTO %s.\"MY_TABLE\" (\"PK\", \"V\") VALUES (?, 1)", ks), i)
              .setConsistencyLevel(ALL);
      session.execute(stmt);
    }
    Cluster cluster = session.getCluster();
    Metadata metadata = cluster.getMetadata();
    TableMetadata table =
        session.getCluster().getMetadata().getKeyspace(ks).getTable("\"MY_TABLE\"");
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, metadata);
    List<TokenRangeReadStatement> statements =
        generator.generate(Runtime.getRuntime().availableProcessors());
    int total = 0;
    for (TokenRangeReadStatement stmt : statements) {
      stmt.setConsistencyLevel(ALL);
      ResultSet rs = session.execute(stmt);
      total += rs.all().size();
      Token token = stmt.getRoutingToken();
      assertThat(stmt.geTokenRange()).isNotNull().endsWith(token.getValue());
      assertThat(rs.getExecutionInfo().getQueriedHost()).isIn(metadata.getReplicas(ks, token));
    }
    assertThat(total).isEqualTo(expectedTotal);
  }
}
