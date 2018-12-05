/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.batch;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.dsbulk.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.Sets;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class StatementBatcherTest {

  protected ByteBuffer key1 = Bytes.fromHexString("0x1234");
  protected ByteBuffer key2 = Bytes.fromHexString("0x5678");
  protected ByteBuffer key3 = Bytes.fromHexString("0x9abc");

  private final Token token1 = Mockito.mock(Token.class);
  private final Token token2 = Mockito.mock(Token.class);

  protected SimpleStatement stmt1 = new SimpleStatement("stmt1").setKeyspace("ks");
  protected SimpleStatement stmt2 = new SimpleStatement("stmt2").setKeyspace("ks");
  protected SimpleStatement stmt3 = new SimpleStatement("stmt3").setKeyspace("ks");
  protected SimpleStatement stmt4 = new SimpleStatement("stmt4").setKeyspace("ks");
  protected SimpleStatement stmt5 = new SimpleStatement("stmt5").setKeyspace("ks");
  protected SimpleStatement stmt6 = new SimpleStatement("stmt6").setKeyspace("ks");

  protected BatchStatement batch12 = new BatchStatement(UNLOGGED).add(stmt1).add(stmt2);
  protected BatchStatement batch126 = new BatchStatement(UNLOGGED).add(stmt1).add(stmt2).add(stmt6);
  protected BatchStatement batch56 = new BatchStatement(UNLOGGED).add(stmt5).add(stmt6);
  protected BatchStatement batch1256 =
      new BatchStatement(UNLOGGED).add(stmt1).add(stmt2).add(stmt5).add(stmt6);
  protected BatchStatement batch34 = new BatchStatement(UNLOGGED).add(stmt3).add(stmt4);

  protected SimpleStatement stmt1WithSize = new SimpleStatement("stmt1", "abcd").setKeyspace("ks");
  protected SimpleStatement stmt2WithSize = new SimpleStatement("stmt2", "efgh").setKeyspace("ks");
  protected SimpleStatement stmt3WithSize = new SimpleStatement("stmt3", "ijkl").setKeyspace("ks");
  protected SimpleStatement stmt4WithSize = new SimpleStatement("stmt4", "jklm").setKeyspace("ks");
  protected SimpleStatement stmt5WithSize = new SimpleStatement("stmt5", "klmn").setKeyspace("ks");
  protected SimpleStatement stmt6WithSize = new SimpleStatement("stmt6", "lmno").setKeyspace("ks");

  protected BatchStatement batch34WithSize = new BatchStatement(UNLOGGED).add(stmt3WithSize).add(stmt4WithSize);
  protected BatchStatement batch56WithSize = new BatchStatement(UNLOGGED).add(stmt5WithSize).add(stmt6WithSize);
  protected BatchStatement batch12WithSize = new BatchStatement(UNLOGGED).add(stmt1WithSize).add(stmt2WithSize);
  protected BatchStatement batch1256WithSize =
      new BatchStatement(UNLOGGED).add(stmt1WithSize).add(stmt2WithSize).add(stmt5WithSize).add(stmt6WithSize);

  protected Cluster cluster;

  private final Host host1 = Mockito.mock(Host.class);
  private final Host host2 = Mockito.mock(Host.class);
  private final Host host3 = Mockito.mock(Host.class);
  private final Host host4 = Mockito.mock(Host.class);

  protected Set<Host> replicaSet1 = Sets.newHashSet(host1, host2, host3);
  protected Set<Host> replicaSet2 = Sets.newHashSet(host2, host3, host4);

  @BeforeEach
  void setUp() throws Exception {
    cluster = Mockito.mock(Cluster.class);
    Configuration configuration = Mockito.mock(Configuration.class);
    ProtocolOptions protocolOptions = Mockito.mock(ProtocolOptions.class);
    Mockito.when(cluster.getConfiguration()).thenReturn(configuration);
    Mockito.when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    Mockito.when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    Mockito.when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  void should_batch_by_routing_key() throws Exception {
    assignRoutingKeys();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token() throws Exception {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_by_replica_set_and_routing_key() throws Exception {
    assignRoutingKeys();
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getReplicas("ks", key1)).thenReturn(replicaSet1);
    Mockito.when(metadata.getReplicas("ks", key2)).thenReturn(replicaSet2);
    Mockito.when(metadata.getReplicas("ks", key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new StatementBatcher(cluster, REPLICA_SET);
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_by_replica_set_and_routing_token() throws Exception {
    assignRoutingTokens();
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getReplicas("ks", key1)).thenReturn(replicaSet1);
    Mockito.when(metadata.getReplicas("ks", key2)).thenReturn(replicaSet2);
    Mockito.when(metadata.getReplicas("ks", key3)).thenReturn(replicaSet1);
    StatementBatcher batcher = new StatementBatcher(cluster, REPLICA_SET);
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_by_routing_key_when_replica_set_info_not_available() throws Exception {
    assignRoutingKeys();
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getReplicas("ks", key1)).thenReturn(new HashSet<>());
    Mockito.when(metadata.getReplicas("ks", key2)).thenReturn(new HashSet<>());
    Mockito.when(metadata.getReplicas("ks", key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new StatementBatcher(cluster, REPLICA_SET);
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  void should_batch_by_routing_token_when_replica_set_info_not_available() throws Exception {
    assignRoutingTokens();
    Metadata metadata = Mockito.mock(Metadata.class);
    Mockito.when(cluster.getMetadata()).thenReturn(metadata);
    Mockito.when(metadata.getReplicas("ks", key1)).thenReturn(new HashSet<>());
    Mockito.when(metadata.getReplicas("ks", key2)).thenReturn(new HashSet<>());
    Mockito.when(metadata.getReplicas("ks", key3)).thenReturn(new HashSet<>());
    StatementBatcher batcher = new StatementBatcher(cluster, REPLICA_SET);
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  void should_batch_all() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).hasSize(1);
    Statement statement = statements.get(0);
    assertThat(((BatchStatement) statement).getStatements())
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  void should_not_batch_one_statement_when_batching_by_routing_key() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchByGroupingKey(stmt1);
    assertThat(statements).containsOnly(stmt1);
  }

  @Test
  void should_not_batch_one_statement_when_batching_all() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchAll(stmt1);
    assertThat(statements).hasSize(1);
    Statement statement = statements.get(0);
    assertThat(statement).isSameAs(stmt1);
  }

  @Test
  void should_honor_max_batch_size() throws Exception {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher(2);
    List<Statement> statements =
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

  protected void assignRoutingTokensWitSize() {
    stmt1WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt2WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt3WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt4WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token2);
    stmt5WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
    stmt6WithSize.setRoutingKey((ByteBuffer) null).setRoutingToken(token1);
  }

}
