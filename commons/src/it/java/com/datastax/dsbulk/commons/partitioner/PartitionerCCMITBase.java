/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.api.core.DefaultConsistencyLevel.ALL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Duration.ONE_MINUTE;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.utils.CQLUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.List;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class PartitionerCCMITBase {

  private static final int EXPECTED_TOTAL = 10000;

  @SuppressWarnings({"unused", "FieldCanBeLocal"})
  private final CCMCluster ccm;

  private final CqlSession session;
  private final boolean multiDc;

  PartitionerCCMITBase(CCMCluster ccm, CqlSession session, boolean multiDc) {
    this.ccm = ccm;
    this.session = session;
    this.multiDc = multiDc;
  }

  @ParameterizedTest(name = "[{index}] rf {0}")
  @ValueSource(ints = {1, 2, 3})
  void should_scan_table(int rf) {
    String ks = createSchema(rf);
    populateTable(ks);
    Metadata metadata = session.getMetadata();
    TokenMap tokenMap = metadata.getTokenMap().orElseThrow(IllegalStateException::new);
    TableMetadata table =
        metadata
            .getKeyspace(ks)
            .orElseThrow(IllegalStateException::new)
            .getTable("\"MY_TABLE\"")
            .orElseThrow(IllegalStateException::new);
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, metadata);
    List<Statement<?>> statements = generator.generate(Runtime.getRuntime().availableProcessors());
    int total = 0;
    for (Statement<?> stmt : statements) {
      stmt = stmt.setConsistencyLevel(ALL).setExecutionProfile(SessionUtils.slowProfile(session));
      ResultSet rs = session.execute(stmt);
      total += rs.all().size();
      Token token = stmt.getRoutingToken();
      assertThat(token).isNotNull();
      assertThat(rs.getExecutionInfo().getCoordinator()).isIn(tokenMap.getReplicas(ks, token));
    }
    assertThat(total).isEqualTo(EXPECTED_TOTAL);
  }

  private String createSchema(int rf) {
    String ks = StringUtils.uniqueIdentifier("MY_KS");
    if (multiDc) {
      session.execute(
          CQLUtils.createKeyspaceNetworkTopologyStrategy(ks, rf, rf)
              .setExecutionProfile(SessionUtils.slowProfile(session)));
    } else {
      session.execute(
          CQLUtils.createKeyspaceSimpleStrategy(ks, rf)
              .setExecutionProfile(SessionUtils.slowProfile(session)));
    }
    ks = CqlIdentifier.fromInternal(ks).asCql(true);
    session.execute(
        SimpleStatement.newInstance(
                String.format(
                    "CREATE TABLE %s.\"MY_TABLE\" (\"PK\" int PRIMARY KEY, \"V\" int)", ks))
            .setExecutionProfile(SessionUtils.slowProfile(session)));
    await().atMost(ONE_MINUTE).until(session::checkSchemaAgreement);
    return ks;
  }

  private void populateTable(String ks) {
    PreparedStatement ps =
        session.prepare(
            String.format("INSERT INTO %s.\"MY_TABLE\" (\"PK\", \"V\") VALUES (?, 1)", ks));
    for (int i = 1; i <= EXPECTED_TOTAL; i++) {
      int attempts = 1;
      while (true) {
        try {
          session.execute(ps.bind(i).setConsistencyLevel(ALL));
          break;
        } catch (RuntimeException e) {
          if (attempts == 3) {
            throw e;
          }
          Uninterruptibles.sleepUninterruptibly(1, SECONDS);
        }
        attempts++;
      }
    }
  }
}
