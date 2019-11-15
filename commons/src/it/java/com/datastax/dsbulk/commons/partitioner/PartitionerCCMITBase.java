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
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.OSS;
import static com.datastax.oss.driver.api.core.DefaultConsistencyLevel.ALL;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_MINUTE;

import com.datastax.dsbulk.commons.tests.ccm.CCMExtension;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.dsbulk.commons.tests.utils.CQLUtils;
import com.datastax.dsbulk.commons.tests.utils.StringUtils;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// restrict the matrix to avoid utilizing too many resources on CI
@CCMRequirements(
    versionRequirements = {
      @CCMVersionRequirement(type = DSE, min = "5.1"),
      @CCMVersionRequirement(type = OSS, min = "3.11")
    })
abstract class PartitionerCCMITBase {

  private static final int EXPECTED_TOTAL = 10_000;
  private static final CqlIdentifier TABLE_NAME = CqlIdentifier.fromInternal("MY_TABLE");

  private final CqlSession session;
  private final boolean multiDc;

  PartitionerCCMITBase(CqlSession session, boolean multiDc) {
    this.session = session;
    this.multiDc = multiDc;
  }

  @ParameterizedTest(name = "[{index}] rf {0}")
  @ValueSource(ints = {1, 2, 3})
  void should_scan_table(int rf) {
    CqlIdentifier ks = createSchema(rf);
    populateTable(ks);
    TableMetadata table = getTable(ks).orElseThrow(IllegalStateException::new);
    TokenRangeReadStatementGenerator generator =
        new TokenRangeReadStatementGenerator(table, session.getMetadata());
    List<Statement<?>> statements = generator.generate(Runtime.getRuntime().availableProcessors());
    int total = 0;
    for (Statement<?> stmt : statements) {
      stmt = stmt.setConsistencyLevel(ALL).setExecutionProfile(SessionUtils.slowProfile(session));
      ResultSet rs = session.execute(stmt);
      for (Row ignored : rs) {
        total++;
      }
    }
    assertThat(total).isEqualTo(EXPECTED_TOTAL);
  }

  private CqlIdentifier createSchema(int rf) {
    CqlIdentifier ks = CqlIdentifier.fromInternal(StringUtils.uniqueIdentifier("MY_KS"));
    if (multiDc) {
      session.execute(
          CQLUtils.createKeyspaceNetworkTopologyStrategy(ks, rf, rf)
              .setExecutionProfile(SessionUtils.slowProfile(session)));
    } else {
      session.execute(
          CQLUtils.createKeyspaceSimpleStrategy(ks, rf)
              .setExecutionProfile(SessionUtils.slowProfile(session)));
    }
    await().atMost(ONE_MINUTE).until(session::checkSchemaAgreement);
    await().atMost(ONE_MINUTE).until(() -> session.getMetadata().getKeyspace(ks).isPresent());
    session.execute(
        SimpleStatement.newInstance(
                String.format(
                    "CREATE TABLE %s.%s (\"PK\" int PRIMARY KEY, \"V\" int)",
                    ks.asCql(true), TABLE_NAME.asCql(true)))
            .setExecutionProfile(SessionUtils.slowProfile(session)));
    await().atMost(ONE_MINUTE).until(session::checkSchemaAgreement);
    await().atMost(ONE_MINUTE).until(() -> getTable(ks).isPresent());
    return ks;
  }

  private Optional<TableMetadata> getTable(CqlIdentifier ks) {
    return session.getMetadata().getKeyspace(ks).flatMap(k -> k.getTable(TABLE_NAME));
  }

  private void populateTable(CqlIdentifier ks) {
    PreparedStatement ps =
        session.prepare(
            String.format(
                "INSERT INTO %s.%s (\"PK\", \"V\") VALUES (?, 1)",
                ks.asCql(true), TABLE_NAME.asCql(true)));
    for (int i = 1; i <= EXPECTED_TOTAL; i++) {
      int attempts = 1;
      while (true) {
        try {
          session.execute(
              ps.bind(i)
                  .setConsistencyLevel(ALL)
                  .setExecutionProfile(SessionUtils.slowProfile(session)));
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
