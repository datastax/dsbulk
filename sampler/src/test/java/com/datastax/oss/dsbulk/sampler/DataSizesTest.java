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
package com.datastax.oss.dsbulk.sampler;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import com.datastax.dse.driver.api.core.DseProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinition;
import com.datastax.oss.driver.internal.core.cql.DefaultColumnDefinitions;
import com.datastax.oss.driver.internal.core.type.codec.IntCodec;
import com.datastax.oss.driver.internal.core.type.codec.StringCodec;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterators;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.protocol.internal.ProtocolConstants.DataType;
import com.datastax.oss.protocol.internal.response.result.ColumnSpec;
import com.datastax.oss.protocol.internal.response.result.RawType;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class DataSizesTest {

  private static final ImmutableMap<String, ByteBuffer> MOCK_PAYLOAD =
      ImmutableMap.of("key1", Bytes.fromHexString("0xabcd"), "key2", Bytes.fromHexString("0xef"));

  @Test
  void should_measure_size_of_simple_statement() {
    String queryString = "SELECT release_version FROM system.local WHERE key = ?";
    SimpleStatement statement = SimpleStatement.newInstance(queryString);
    int expectedSize = 0;
    assertThat(
            DataSizes.getDataSize(
                statement, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(expectedSize);

    SimpleStatement statementWithPositionalValue =
        SimpleStatement.newInstance(statement.getQuery(), "local");
    assertThat(
            DataSizes.getDataSize(
                statementWithPositionalValue,
                DseProtocolVersion.DSE_V2,
                DefaultCodecRegistry.DEFAULT))
        .isEqualTo(
            expectedSize + "local".getBytes(StandardCharsets.UTF_8).length // value
            );

    SimpleStatement statementWithNamedValue =
        SimpleStatement.newInstance(
            statement.getQuery(), ImmutableMap.of("key", "local")); // key not taken into account

    assertThat(
            DataSizes.getDataSize(
                statementWithNamedValue, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(
            expectedSize + "local".getBytes(StandardCharsets.UTF_8).length // value
            );

    statement = statement.setCustomPayload(MOCK_PAYLOAD);
    assertThat(
            DataSizes.getDataSize(
                statement, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(expectedSize); // payload not taken into account
  }

  @Test
  void should_measure_size_of_bound_statement() {
    BoundStatement bs = mockBoundStatement(null, null);

    int expectedSize = 0;
    assertThat(DataSizes.getDataSize(bs, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(expectedSize);

    bs = mockBoundStatement(0, "test");
    expectedSize = bs.getBytesUnsafe(0).remaining() + bs.getBytesUnsafe(1).remaining();
    assertThat(DataSizes.getDataSize(bs, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(expectedSize);

    verify(bs, never()).getPagingState();
    verify(bs, never()).getCustomPayload();
  }

  @Test
  void should_measure_size_of_batch_statement() {

    SimpleStatement stmt1 = SimpleStatement.newInstance("SELECT release_version FROM system.local");
    BoundStatement stmt2 = mockBoundStatement(1, "test1");
    BoundStatement stmt3 = mockBoundStatement(2, "test2");

    BatchStatement batch = mockBatchStatement(stmt1, stmt2, stmt3);

    int expectedSize =
        4 // setInt(1)
            + 4 // setInt(2)
            + "test1".getBytes(StandardCharsets.UTF_8).length
            + "test2".getBytes(StandardCharsets.UTF_8).length;
    assertThat(
            DataSizes.getDataSize(batch, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(expectedSize);

    stmt1 = stmt1.setCustomPayload(MOCK_PAYLOAD);
    batch = mockBatchStatement(stmt1, stmt2, stmt3);
    assertThat(
            DataSizes.getDataSize(batch, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(expectedSize); // payload not taken into account

    verify(batch, never()).getPagingState();
    verify(batch, never()).getCustomPayload();
    verify(stmt2, never()).getPagingState();
    verify(stmt2, never()).getCustomPayload();
    verify(stmt3, never()).getPagingState();
    verify(stmt3, never()).getCustomPayload();
  }

  @Test
  void should_measure_size_of_row() {
    Row row = mock(Row.class);
    when(row.getColumnDefinitions()).thenReturn(mockColumnDefinitions());
    when(row.getBytesUnsafe(0)).thenReturn(Bytes.fromHexString("0xCAFEBABE"));
    when(row.getBytesUnsafe(1)).thenReturn(ByteBuffer.wrap(new byte[0]));
    // 1st col: 4 bytes + 2nd col: 0 bytes
    assertThat(DataSizes.getDataSize(row)).isEqualTo(4);
  }

  @Test
  void should_measure_size_of_sizeable_statement() {
    Sizeable stmt = mock(Sizeable.class, withSettings().extraInterfaces(Statement.class));
    when(stmt.getDataSize()).thenReturn(1234L);
    assertThat(
            DataSizes.getDataSize(
                (Statement<?>) stmt, DseProtocolVersion.DSE_V2, DefaultCodecRegistry.DEFAULT))
        .isEqualTo(1234L);
  }

  @Test
  void should_measure_size_of_sizeable_row() {
    Sizeable row = mock(Sizeable.class, withSettings().extraInterfaces(Row.class));
    when(row.getDataSize()).thenReturn(1234L);
    assertThat(DataSizes.getDataSize((Row) row)).isEqualTo(1234L);
  }

  private BatchStatement mockBatchStatement(BatchableStatement<?>... statements) {
    BatchStatement batch = mock(BatchStatement.class);
    when(batch.iterator()).thenAnswer(args -> Iterators.forArray(statements));
    return batch;
  }

  private BoundStatement mockBoundStatement(Integer col1, String col2) {
    PreparedStatement ps = mockPreparedStatement();
    BoundStatement bs = mock(BoundStatement.class);
    when(bs.getPreparedStatement()).thenReturn(ps);
    ByteBuffer col1bb = new IntCodec().encode(col1, DseProtocolVersion.DSE_V2);
    ByteBuffer col2bb =
        new StringCodec(DataTypes.TEXT, StandardCharsets.UTF_8)
            .encode(col2, DseProtocolVersion.DSE_V2);
    when(bs.getBytesUnsafe(0)).thenReturn(col1bb);
    when(bs.getBytesUnsafe(1)).thenReturn(col2bb);
    return bs;
  }

  private PreparedStatement mockPreparedStatement() {
    ColumnDefinitions columnDefinitions = mockColumnDefinitions();
    PreparedStatement ps = mock(PreparedStatement.class);
    when(ps.getVariableDefinitions()).thenReturn(columnDefinitions);
    return ps;
  }

  private ColumnDefinitions mockColumnDefinitions() {
    return DefaultColumnDefinitions.valueOf(
        Lists.newArrayList(
            new DefaultColumnDefinition(
                new ColumnSpec("ks", "table", "c1", 0, RawType.PRIMITIVES.get(DataType.INT)),
                AttachmentPoint.NONE),
            new DefaultColumnDefinition(
                new ColumnSpec("ks", "table", "c2", 1, RawType.PRIMITIVES.get(DataType.VARCHAR)),
                AttachmentPoint.NONE)));
  }
}
