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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.protocol.internal.util.Bytes;
import com.google.common.collect.Sets;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class StatementBatcherTest {

  protected ByteBuffer key1 = Bytes.fromHexString("0x1234");
  protected ByteBuffer key2 = Bytes.fromHexString("0x5678");
  protected ByteBuffer key3 = Bytes.fromHexString("0x9abc");

  private final Token token1 = mock(Token.class);
  private final Token token2 = mock(Token.class);

  protected SimpleStatement stmt1 = SimpleStatement.newInstance("stmt1").setKeyspace("ks");
  protected SimpleStatement stmt2 = SimpleStatement.newInstance("stmt2").setKeyspace("ks");
  protected SimpleStatement stmt3 = SimpleStatement.newInstance("stmt3").setKeyspace("ks");
  protected SimpleStatement stmt4 = SimpleStatement.newInstance("stmt4").setKeyspace("ks");
  protected SimpleStatement stmt5 = SimpleStatement.newInstance("stmt5").setKeyspace("ks");
  protected SimpleStatement stmt6 = SimpleStatement.newInstance("stmt6").setKeyspace("ks");

  protected BatchStatement batch12 = BatchStatement.newInstance(UNLOGGED).add(stmt1).add(stmt2);
  protected BatchStatement batch126 =
      BatchStatement.newInstance(UNLOGGED).add(stmt1).add(stmt2).add(stmt6);
  protected BatchStatement batch56 = BatchStatement.newInstance(UNLOGGED).add(stmt5).add(stmt6);
  protected BatchStatement batch1256 =
      BatchStatement.newInstance(UNLOGGED).add(stmt1).add(stmt2).add(stmt5).add(stmt6);
  protected BatchStatement batch34 = BatchStatement.newInstance(UNLOGGED).add(stmt3).add(stmt4);

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
  }

  @Test
  void should_batch_by_routing_key() {
    assignRoutingKeys();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token() {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_by_replica_set_and_routing_key() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas("ks", key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas("ks", key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas("ks", key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_by_replica_set_and_routing_token() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas("ks", key1)).thenReturn(replicaSet1);
    when(tokenMap.getReplicas("ks", key2)).thenReturn(replicaSet2);
    when(tokenMap.getReplicas("ks", key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_by_routing_key_when_replica_set_info_not_available() {
    assignRoutingKeys();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas("ks", key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas("ks", key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas("ks", key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token_when_replica_set_info_not_available() {
    assignRoutingTokens();
    Metadata metadata = mock(Metadata.class);
    TokenMap tokenMap = mock(TokenMap.class);
    when(session.getMetadata()).thenReturn(metadata);
    when(metadata.getTokenMap()).thenReturn(Optional.of(tokenMap));
    when(tokenMap.getReplicas("ks", key1)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas("ks", key2)).thenReturn(new HashSet<>());
    when(tokenMap.getReplicas("ks", key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new StatementBatcher(session, REPLICA_SET);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
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
  void should_honor_max_batch_size() {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher(2);
    List<Statement<?>> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch12, batch56, batch34);
    statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch12, batch56, batch34);
  }

  protected void assignRoutingKeys() {
    stmt1.setRoutingKey(key1).setRoutingToken(null);
    stmt2.setRoutingKey(key1).setRoutingToken(null);
    stmt3.setRoutingKey(key2).setRoutingToken(null);
    stmt4.setRoutingKey(key2).setRoutingToken(null);
    stmt5.setRoutingKey(key3).setRoutingToken(null);
    stmt6.setRoutingKey(key1).setRoutingToken(null);
  }

  protected void assignRoutingTokens() {
    stmt1.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt2.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt3.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt4.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt5.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt6.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
  }
}
