/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.batch;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static com.datastax.loader.executor.api.batch.StatementBatcher.BatchMode.REPLICA_SET;
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
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

/** */
public class StatementBatcherTest {

  ByteBuffer key1 = Bytes.fromHexString("0x1234");
  ByteBuffer key2 = Bytes.fromHexString("0x5678");
  ByteBuffer key3 = Bytes.fromHexString("0x9abc");

  Token token1 = Mockito.mock(Token.class);
  Token token2 = Mockito.mock(Token.class);

  SimpleStatement stmt1 = new SimpleStatement("stmt1").setKeyspace("ks");
  SimpleStatement stmt2 = new SimpleStatement("stmt2").setKeyspace("ks");
  SimpleStatement stmt3 = new SimpleStatement("stmt3").setKeyspace("ks");
  SimpleStatement stmt4 = new SimpleStatement("stmt4").setKeyspace("ks");
  SimpleStatement stmt5 = new SimpleStatement("stmt5").setKeyspace("ks");
  SimpleStatement stmt6 = new SimpleStatement("stmt6").setKeyspace("ks");

  BatchStatement batch12 = new BatchStatement(UNLOGGED).add(stmt1).add(stmt2);
  BatchStatement batch126 = new BatchStatement(UNLOGGED).add(stmt1).add(stmt2).add(stmt6);
  BatchStatement batch1256 =
      new BatchStatement(UNLOGGED).add(stmt1).add(stmt2).add(stmt5).add(stmt6);
  BatchStatement batch34 = new BatchStatement(UNLOGGED).add(stmt3).add(stmt4);

  Cluster cluster;

  Host host1 = Mockito.mock(Host.class);
  Host host2 = Mockito.mock(Host.class);
  Host host3 = Mockito.mock(Host.class);
  Host host4 = Mockito.mock(Host.class);

  Set<Host> replicaSet1 = Sets.newHashSet(host1, host2, host3);
  Set<Host> replicaSet2 = Sets.newHashSet(host2, host3, host4);

  @Before
  public void setUp() throws Exception {
    cluster = Mockito.mock(Cluster.class);
    Configuration configuration = Mockito.mock(Configuration.class);
    ProtocolOptions protocolOptions = Mockito.mock(ProtocolOptions.class);
    Mockito.when(cluster.getConfiguration()).thenReturn(configuration);
    Mockito.when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    Mockito.when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    Mockito.when(configuration.getCodecRegistry()).thenReturn(CodecRegistry.DEFAULT_INSTANCE);
  }

  @Test
  public void should_batch_by_routing_key() throws Exception {
    assignRoutingKeys();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  public void should_batch_by_routing_token() throws Exception {
    assignRoutingTokens();
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements =
        batcher.batchByGroupingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch1256, batch34);
  }

  @Test
  public void should_batch_by_replica_set_and_routing_key() throws Exception {
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
  public void should_batch_by_replica_set_and_routing_token() throws Exception {
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
  public void should_batch_by_routing_key_when_replica_set_info_not_available() throws Exception {
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
  public void should_batch_by_routing_token_when_replica_set_info_not_available() throws Exception {
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
  public void should_batch_all() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchAll(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).hasSize(1);
    Statement statement = statements.get(0);
    assertThat(((BatchStatement) statement).getStatements())
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  public void should_not_batch_one_statement_when_batching_by_routing_key() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchByGroupingKey(stmt1);
    assertThat(statements).containsOnly(stmt1);
  }

  @Test
  public void should_not_batch_one_statement_when_batching_all() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchAll(stmt1);
    assertThat(statements).hasSize(1);
    Statement statement = statements.get(0);
    assertThat(statement).isSameAs(stmt1);
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
