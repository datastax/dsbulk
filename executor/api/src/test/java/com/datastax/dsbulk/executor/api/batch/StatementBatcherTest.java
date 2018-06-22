/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.batch;

import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static com.datastax.oss.driver.api.core.cql.DefaultBatchType.UNLOGGED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.tuple;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.Sets;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.assertj.core.api.iterable.ThrowingExtractor;
import org.assertj.core.groups.Tuple;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatementBatcherTest {

  protected static final ThrowingExtractor<Statement<?>, Tuple, RuntimeException> EXTRACTOR =
      stmt -> {
        if (stmt instanceof SimpleStatement) {
          return tuple(stmt);
        } else {
          List<Statement<?>> children = new ArrayList<>();
          for (BatchableStatement<?> child : ((BatchStatement) stmt)) {
            children.add(child);
          }
          return tuple(children.toArray());
        }
      };

  protected ByteBuffer key1 = Bytes.fromHexString("0x1234");
  protected ByteBuffer key2 = Bytes.fromHexString("0x5678");
  protected ByteBuffer key3 = Bytes.fromHexString("0x9abc");

  private final Token token1 = mock(Token.class);
  private final Token token2 = mock(Token.class);

  protected final CqlIdentifier ks = CqlIdentifier.fromInternal("ks");

  protected SimpleStatement stmt1 = SimpleStatement.newInstance("stmt1").setKeyspace(ks);
  protected SimpleStatement stmt2 = SimpleStatement.newInstance("stmt2").setKeyspace(ks);
  protected SimpleStatement stmt3 = SimpleStatement.newInstance("stmt3").setKeyspace(ks);
  protected SimpleStatement stmt4 = SimpleStatement.newInstance("stmt4").setKeyspace(ks);
  protected SimpleStatement stmt5 = SimpleStatement.newInstance("stmt5").setKeyspace(ks);
  protected SimpleStatement stmt6 = SimpleStatement.newInstance("stmt6").setKeyspace(ks);

  protected SimpleStatement stmt1WithSize =
      SimpleStatement.newInstance("stmt1", "abcd").setKeyspace("ks");
  protected SimpleStatement stmt2WithSize =
      SimpleStatement.newInstance("stmt2", "efgh").setKeyspace("ks");
  protected SimpleStatement stmt3WithSize =
      SimpleStatement.newInstance("stmt3", "ijkl").setKeyspace("ks");
  protected SimpleStatement stmt4WithSize =
      SimpleStatement.newInstance("stmt4", "jklm").setKeyspace("ks");
  protected SimpleStatement stmt5WithSize =
      SimpleStatement.newInstance("stmt5", "klmn").setKeyspace("ks");
  protected SimpleStatement stmt6WithSize =
      SimpleStatement.newInstance("stmt6", "lmno").setKeyspace("ks");

  protected BatchStatement batch34WithSize =
      BatchStatement.newInstance(UNLOGGED).add(stmt3WithSize).add(stmt4WithSize);
  protected BatchStatement batch56WithSize =
      BatchStatement.newInstance(UNLOGGED).add(stmt5WithSize).add(stmt6WithSize);
  protected BatchStatement batch12WithSize =
      BatchStatement.newInstance(UNLOGGED).add(stmt1WithSize).add(stmt2WithSize);
  protected BatchStatement batch1256WithSize =
      BatchStatement.newInstance(UNLOGGED)
          .add(stmt1WithSize)
          .add(stmt2WithSize)
          .add(stmt5WithSize)
          .add(stmt6WithSize);
  protected BatchStatement batch123456WithSize =
      BatchStatement.newInstance(UNLOGGED)
          .add(stmt1WithSize)
          .add(stmt2WithSize)
          .add(stmt3WithSize)
          .add(stmt4WithSize)
          .add(stmt5WithSize)
          .add(stmt6WithSize);

  protected CqlSession session;

  private final Node node1 = mock(Node.class);
  private final Node node2 = mock(Node.class);
  private final Node node3 = mock(Node.class);
  private final Node node4 = mock(Node.class);

  protected Set<Node> replicaSet1 = Sets.newHashSet(node1, node2, node3);
  protected Set<Node> replicaSet2 = Sets.newHashSet(node2, node3, node4);

  @BeforeEach
  void setUp() {
    session = mock(CqlSession.class);
    DriverContext context = mock(DriverContext.class);
    when(session.getContext()).thenReturn(context);
    when(context.getProtocolVersion()).thenReturn(ProtocolVersion.DEFAULT);
    when(context.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT);
  }

  @Test
  void should_batch_by_routing_key() {
    assignRoutingKeys();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token() {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_replica_set_and_routing_key() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    //noinspection unchecked
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_replica_set_and_routing_token() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    //noinspection unchecked
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas(ks, key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas(ks, key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_by_routing_key_when_replica_set_info_not_available() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    //noinspection unchecked
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt6), tuple(stmt3, stmt4), tuple(stmt5));
  }

  @Test
  void should_batch_by_routing_token_when_replica_set_info_not_available() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    //noinspection unchecked
    when(metadata.getTokenMap()).thenReturn((Optional) Optional.of(tokenMap));
    when(tokenMap.getReplicas(ks, key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas(ks, key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2, stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_batch_all() {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).hasSize(1);
    Statement statement = statements.get(0);
    assertThat(((BatchStatement) statement))
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  void should_not_batch_one_statement_when_batching_by_routing_key() {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements = batcher.batchByGroupingKey(stmt1);
    assertThat(statements).containsOnly(stmt1);
  }

  @Test
  void should_not_batch_one_statement_when_batching_all() {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements = batcher.batchAll(stmt1);
    assertThat(statements).hasSize(1);
    Statement statement = statements.get(0);
    assertThat(statement).isSameAs(stmt1);
  }

  @Test
  void should_honor_max_statements_in_batch() {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher(2);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(tuple(stmt1, stmt2), tuple(stmt5, stmt6), tuple(stmt3, stmt4));
  }

  @Test
  void should_honor_max_size_in_bytes() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new StatementBatcher(8L);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        batcher.batchAll(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_high() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new StatementBatcher(1000);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize, stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        batcher.batchAll(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(
                stmt1WithSize,
                stmt2WithSize,
                stmt3WithSize,
                stmt4WithSize,
                stmt5WithSize,
                stmt6WithSize));
  }

  @Test
  void should_buffer_by_max_size_in_bytes_if_satisfied_before_max_batch_statements() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new StatementBatcher(10, 8L);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        batcher.batchAll(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize),
            tuple(stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
  }

  @Test
  void should_buffer_by_max_batch_statements_if_satisfied_before_max_size_in_bytes() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new StatementBatcher(1, 8L);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize),
            tuple(stmt2WithSize),
            tuple(stmt3WithSize),
            tuple(stmt4WithSize),
            tuple(stmt5WithSize),
            tuple(stmt6WithSize));
    statements =
        batcher.batchAll(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize),
            tuple(stmt2WithSize),
            tuple(stmt3WithSize),
            tuple(stmt4WithSize),
            tuple(stmt5WithSize),
            tuple(stmt6WithSize));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_high() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new StatementBatcher(100, 1000);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize, stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        batcher.batchAll(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(
                stmt1WithSize,
                stmt2WithSize,
                stmt3WithSize,
                stmt4WithSize,
                stmt5WithSize,
                stmt6WithSize));
  }

  @Test
  void should_buffer_until_last_element_if_max_size_in_bytes_and_max_batch_statements_negative() {
    assignRoutingTokensWitSize();
    StatementBatcher batcher = new StatementBatcher(-1, -1);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(stmt1WithSize, stmt2WithSize, stmt5WithSize, stmt6WithSize),
            tuple(stmt3WithSize, stmt4WithSize));
    statements =
        batcher.batchAll(
            stmt1WithSize,
            stmt2WithSize,
            stmt3WithSize,
            stmt4WithSize,
            stmt5WithSize,
            stmt6WithSize);
    assertThat(statements)
        .extracting(EXTRACTOR)
        .contains(
            tuple(
                stmt1WithSize,
                stmt2WithSize,
                stmt3WithSize,
                stmt4WithSize,
                stmt5WithSize,
                stmt6WithSize));
  }

  protected void assignRoutingKeys() {
    stmt1 = stmt1.setRoutingKey(key1).setRoutingToken(null);
    stmt2 = stmt2.setRoutingKey(key1).setRoutingToken(null);
    stmt3 = stmt3.setRoutingKey(key2).setRoutingToken(null);
    stmt4 = stmt4.setRoutingKey(key2).setRoutingToken(null);
    stmt5 = stmt5.setRoutingKey(key3).setRoutingToken(null);
    stmt6 = stmt6.setRoutingKey(key1).setRoutingToken(null);
  }

  protected void assignRoutingTokens() {
    stmt1 = stmt1.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt2 = stmt2.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt3 = stmt3.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt4 = stmt4.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt5 = stmt5.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt6 = stmt6.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
  }

  protected void assignRoutingTokensWitSize() {
    stmt1WithSize = stmt1WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt2WithSize = stmt2WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt3WithSize = stmt3WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt4WithSize = stmt4WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt5WithSize = stmt5WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt6WithSize = stmt6WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
  }
}
