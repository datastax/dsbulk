/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.partitioner;

import static com.datastax.oss.driver.api.core.DefaultConsistencyLevel.ALL;
import static com.datastax.oss.dsbulk.partitioner.assertions.PartitionerAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type.OSS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.fail;
import static org.awaitility.Awaitility.await;
import static org.awaitility.Durations.ONE_MINUTE;
import static org.junit.jupiter.api.Assumptions.assumeFalse;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.testinfra.session.SessionUtils;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster;
import com.datastax.oss.dsbulk.tests.ccm.CCMCluster.Type;
import com.datastax.oss.dsbulk.tests.ccm.CCMExtension;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMRequirements;
import com.datastax.oss.dsbulk.tests.ccm.annotations.CCMVersionRequirement;
import com.datastax.oss.dsbulk.tests.driver.VersionUtils;
import com.datastax.oss.dsbulk.tests.utils.CQLUtils;
import com.datastax.oss.dsbulk.tests.utils.StringUtils;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// restrict the matrix to avoid utilizing too many resources on CI
@CCMRequirements(
    versionRequirements = {
      @CCMVersionRequirement(type = DSE, min = "5.1"),
      @CCMVersionRequirement(type = OSS, min = "3.11")
    })
abstract class PartitionerCCMITBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(PartitionerCCMITBase.class);

  private static final int EXPECTED_TOTAL = 10_000;
  private static final CqlIdentifier TABLE_NAME = CqlIdentifier.fromInternal("MY_TABLE");

  private static final Version DSE_6_0 = Objects.requireNonNull(Version.parse("6.0.0"));
  private static final Version DSE_6_8 = Objects.requireNonNull(Version.parse("6.8.0"));

  private final CCMCluster ccm;
  private final CqlSession session;
  private final boolean multiDc;

  PartitionerCCMITBase(CCMCluster ccm, CqlSession session, boolean multiDc) {
    this.ccm = ccm;
    this.session = session;
    this.multiDc = multiDc;
  }

  @ParameterizedTest(name = "[{index}] rf {0} splitCount {1}")
  @MethodSource
  void should_scan_table(int rf, int splitCount) {

    // TODO remove when DB-4412 is fixed
    assumeFalse(
        ccm.getClusterType() == Type.DSE
            && VersionUtils.isWithinRange(ccm.getVersion(), DSE_6_0, DSE_6_8),
        "This test fails frequently for DSE 6.0 and 6.7, see https://datastax.jira.com/browse/DB-4412");

    try {
      CqlIdentifier ks = createSchema(rf);
      populateTable(ks);
      TableMetadata table = getTable(ks).orElseThrow(IllegalStateException::new);
      TokenRangeReadStatementGenerator generator =
          new TokenRangeReadStatementGenerator(table, session.getMetadata());
      List<Statement<?>> statements = generator.generate(splitCount);
      int total = 0;
      TokenMap tokenMap = session.getMetadata().getTokenMap().get();
      for (Statement<?> stmt : statements) {
        stmt = stmt.setConsistencyLevel(ALL).setExecutionProfile(SessionUtils.slowProfile(session));
        ResultSet rs = null;
        try {
          rs = session.execute(stmt);
        } catch (Exception e) {
          String failureMessage =
              "Could not execute statement: " + ((SimpleStatement) stmt).getQuery();
          LOGGER.error(failureMessage);
          LOGGER.error("table definition: ");
          LOGGER.error(table.describe(true));
          LOGGER.error("keyspace definition: ");
          session
              .getMetadata()
              .getKeyspace(ks)
              .ifPresent(keyspace -> LOGGER.error(keyspace.describe(true)));
          fail(failureMessage, e);
        }
        for (@SuppressWarnings("unused") Row ignored : rs) {
          total++;
        }
        Token routingToken = stmt.getRoutingToken();
        assertThat(routingToken).isNotNull();
        Set<Node> replicas = tokenMap.getReplicas(ks, routingToken);
        assertThat(rs.getExecutionInfo().getCoordinator()).isIn(replicas);
      }
      assertThat(total).isEqualTo(EXPECTED_TOTAL);
    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }
  }

  @SuppressWarnings("unused")
  static Stream<Arguments> should_scan_table() {
    return Stream.of(
        arguments(1, 1),
        arguments(1, 2),
        arguments(1, Runtime.getRuntime().availableProcessors()),
        arguments(1, 1000),
        arguments(2, 1),
        arguments(2, 2),
        arguments(2, Runtime.getRuntime().availableProcessors()),
        arguments(2, 1000),
        arguments(3, 1),
        arguments(3, 2),
        arguments(3, Runtime.getRuntime().availableProcessors()),
        arguments(3, 1000));
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
