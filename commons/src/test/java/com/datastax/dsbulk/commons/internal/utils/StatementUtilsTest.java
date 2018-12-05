/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.DriverCoreCommonsTestHooks;
import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StatementUtilsTest {
  private static final byte[] MOCK_PAGING_STATE = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));
  private static final ByteBuffer MOCK_PAYLOAD_VALUE1 = Bytes.fromHexString("0xabcd");
  private static final ByteBuffer MOCK_PAYLOAD_VALUE2 = Bytes.fromHexString("0xef");
  private static final ImmutableMap<String, ByteBuffer> MOCK_PAYLOAD =
      ImmutableMap.of("key1", MOCK_PAYLOAD_VALUE1, "key2", MOCK_PAYLOAD_VALUE2);

  @Mock private PreparedStatement preparedStatement;

  @BeforeAll
  void setup() {
    MockitoAnnotations.initMocks(this);

    ColumnDefinitions columnDefinitions =
        DriverCoreCommonsTestHooks.newColumnDefinitions(
            DriverCoreCommonsTestHooks.newDefinition("ks", "table", "c1", DataType.cint()),
            DriverCoreCommonsTestHooks.newDefinition("ks", "table", "c2", DataType.text()));

    PreparedId preparedId =
        DriverCoreCommonsTestHooks.newPreparedId(
            columnDefinitions, new int[0], ProtocolVersion.DSE_V2);
    Mockito.when(preparedStatement.getPreparedId()).thenReturn(preparedId);

    Mockito.when(preparedStatement.getVariables()).thenReturn(columnDefinitions);
    Mockito.when(preparedStatement.getIncomingPayload()).thenReturn(null);
    Mockito.when(preparedStatement.getOutgoingPayload()).thenReturn(null);
    Mockito.when(preparedStatement.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  void should_measure_size_of_simple_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    SimpleStatement statement = new SimpleStatement(queryString);
    int expectedSize = 0;
    assertThat(getDataSize(statement)).isEqualTo(expectedSize);

    SimpleStatement statementWithAnonymousValue =
        new SimpleStatement(statement.getQueryString(), "local");
    assertThat(getDataSize(statementWithAnonymousValue))
        .isEqualTo(
            expectedSize + "local".getBytes(Charsets.UTF_8).length // value
            );

    SimpleStatement statementWithNamedValue =
        new SimpleStatement(
            statement.getQueryString(),
            ImmutableMap.of("key", "local")); // key not taken into account

    assertThat(getDataSize(statementWithNamedValue))
        .isEqualTo(
            expectedSize + "local".getBytes(Charsets.UTF_8).length // value
            );

    statement.setOutgoingPayload(MOCK_PAYLOAD);
    assertThat(getDataSize(statement)).isEqualTo(expectedSize); // payload not taken into account
  }

  @Test
  void should_measure_size_of_bound_statement() {
    BoundStatement statement = new BoundStatement(preparedStatement);
    int expectedSize = 0;
    assertThat(getDataSize(statement)).isEqualTo(expectedSize);

    statement.setInt(0, 0);
    expectedSize += 4; // serialized value (we already have its size from when it was null above)
    statement.setString(1, "test");
    expectedSize += "test".getBytes(Charsets.UTF_8).length;
    assertThat(getDataSize(statement)).isEqualTo(expectedSize);

    statement.setPagingStateUnsafe(MOCK_PAGING_STATE); // not taken into account
    assertThat(getDataSize(statement)).isEqualTo(expectedSize);

    statement.setOutgoingPayload(MOCK_PAYLOAD); // not taken into account
    assertThat(getDataSize(statement)).isEqualTo(expectedSize);
  }

  @Test
  void should_measure_size_of_batch_statement() {
    String queryString = "SELECT release_version FROM system.local";
    SimpleStatement statement1 = new SimpleStatement(queryString);

    BoundStatement statement2 =
        new BoundStatement(preparedStatement).setInt(0, 0).setString(1, "test");
    BoundStatement statement3 =
        new BoundStatement(preparedStatement).setInt(0, 0).setString(1, "test2");

    BatchStatement batchStatement =
        new BatchStatement().add(statement1).add(statement2).add(statement3);

    int expectedSize =
        4 // setInt(0)
            + 4 // setInt(0)
            + "test".getBytes(Charsets.UTF_8).length
            + "test2".getBytes(Charsets.UTF_8).length;
    assertThat(getDataSize(batchStatement)).isEqualTo(expectedSize);

    batchStatement.setOutgoingPayload(MOCK_PAYLOAD); // not taken into account
    assertThat(getDataSize(batchStatement)).isEqualTo(expectedSize);
  }

  @Test
  void should_measure_size_of_wrapped_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    Statement statement = new StatementWrapper(new SimpleStatement(queryString)) {};
    int expectedSize = 0;
    assertThat(getDataSize(statement)).isEqualTo(expectedSize);

    SimpleStatement statementWithAnonymousValue = new SimpleStatement(queryString, "local");
    statement = new StatementWrapper(statementWithAnonymousValue) {};

    assertThat(getDataSize(statement))
        .isEqualTo(expectedSize + "local".getBytes(Charsets.UTF_8).length);
  }

  private long getDataSize(Statement statement) {
    return StatementUtils.getDataSize(
        statement, ProtocolVersion.DSE_V2, CodecRegistry.DEFAULT_INSTANCE);
  }
}
