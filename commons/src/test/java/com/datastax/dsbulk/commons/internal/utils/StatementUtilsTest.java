/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import static com.datastax.driver.core.CodecRegistry.DEFAULT_INSTANCE;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newColumnDefinitions;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newDefinition;
import static com.datastax.driver.core.DriverCoreCommonsTestHooks.newPreparedId;
import static com.datastax.driver.core.ProtocolVersion.DSE_V2;
import static com.google.common.base.Charsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableMap;
import java.nio.ByteBuffer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StatementUtilsTest {

  private static final byte[] MOCK_PAGING_STATE = Bytes.getArray(Bytes.fromHexString("0xdeadbeef"));
  private static final ByteBuffer MOCK_PAYLOAD_VALUE1 = Bytes.fromHexString("0xabcd");
  private static final ByteBuffer MOCK_PAYLOAD_VALUE2 = Bytes.fromHexString("0xef");
  private static final ImmutableMap<String, ByteBuffer> MOCK_PAYLOAD =
      ImmutableMap.of("key1", MOCK_PAYLOAD_VALUE1, "key2", MOCK_PAYLOAD_VALUE2);

  @Mock private PreparedStatement preparedStatement;
  @Mock private Row row;

  @BeforeAll
  void setup() {
    MockitoAnnotations.initMocks(this);

    ColumnDefinitions columnDefinitions =
        newColumnDefinitions(
            newDefinition("ks", "table", "c1", DataType.cint()),
            newDefinition("ks", "table", "c2", DataType.text()));

    PreparedId preparedId = newPreparedId(columnDefinitions, new int[0], DSE_V2);
    when(preparedStatement.getPreparedId()).thenReturn(preparedId);
    when(preparedStatement.getVariables()).thenReturn(columnDefinitions);
    when(preparedStatement.getCodecRegistry()).thenReturn(DEFAULT_INSTANCE);
    when(row.getColumnDefinitions()).thenReturn(columnDefinitions);
  }

  @Test
  void should_measure_size_of_simple_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    SimpleStatement statement = new SimpleStatement(queryString);
    int expectedSize = 0;
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);

    SimpleStatement statementWithAnonymousValue =
        new SimpleStatement(statement.getQueryString(), "local");
    assertThat(StatementUtils.getDataSize(statementWithAnonymousValue, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(
            expectedSize + "local".getBytes(UTF_8).length // value
            );

    SimpleStatement statementWithNamedValue =
        new SimpleStatement(
            statement.getQueryString(),
            ImmutableMap.of("key", "local")); // key not taken into account

    assertThat(StatementUtils.getDataSize(statementWithNamedValue, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(
            expectedSize + "local".getBytes(UTF_8).length // value
            );

    statement.setOutgoingPayload(MOCK_PAYLOAD);
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize); // payload not taken into account
  }

  @Test
  void should_measure_size_of_bound_statement() {
    BoundStatement statement = new BoundStatement(preparedStatement);
    int expectedSize = 0;
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);

    statement.setInt(0, 0);
    expectedSize += 4; // serialized value (we already have its size from when it was null above)
    statement.setString(1, "test");
    expectedSize += "test".getBytes(UTF_8).length;
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);

    statement.setPagingStateUnsafe(MOCK_PAGING_STATE); // not taken into account
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);

    statement.setOutgoingPayload(MOCK_PAYLOAD); // not taken into account
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);
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
            + "test".getBytes(UTF_8).length
            + "test2".getBytes(UTF_8).length;
    assertThat(StatementUtils.getDataSize(batchStatement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);

    batchStatement.setOutgoingPayload(MOCK_PAYLOAD); // not taken into account
    assertThat(StatementUtils.getDataSize(batchStatement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);
  }

  @Test
  void should_measure_size_of_wrapped_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    Statement statement = new StatementWrapper(new SimpleStatement(queryString)) {};
    int expectedSize = 0;
    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize);

    SimpleStatement statementWithAnonymousValue = new SimpleStatement(queryString, "local");
    statement = new StatementWrapper(statementWithAnonymousValue) {};

    assertThat(StatementUtils.getDataSize(statement, DSE_V2, DEFAULT_INSTANCE))
        .isEqualTo(expectedSize + "local".getBytes(UTF_8).length);
  }

  @Test
  void should_measure_size_of_row() {
    when(row.getBytesUnsafe(0)).thenReturn(Bytes.fromHexString("0xCAFEBABE"));
    when(row.getBytesUnsafe(1)).thenReturn(ByteBuffer.wrap(new byte[0]));
    // 1st col: 4 bytes + 2nd col: 0 bytes
    assertThat(StatementUtils.getDataSize(row)).isEqualTo(4);
  }
}
