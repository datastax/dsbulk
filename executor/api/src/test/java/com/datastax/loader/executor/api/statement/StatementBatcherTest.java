/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.statement;

import static com.datastax.driver.core.BatchStatement.Type.UNLOGGED;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.utils.Bytes;
import java.nio.ByteBuffer;
import java.util.List;
import org.junit.Test;

/** */
public class StatementBatcherTest {

  static ByteBuffer key1 = Bytes.fromHexString("0x1234");
  static ByteBuffer key2 = Bytes.fromHexString("0x5678");
  static ByteBuffer key3 = Bytes.fromHexString("0x9abc");

  static Statement stmt1 = new SimpleStatement("stmt1").setRoutingKey(key1);
  static Statement stmt2 = new SimpleStatement("stmt2").setRoutingKey(key1);
  static Statement stmt3 = new SimpleStatement("stmt3").setRoutingKey(key2);
  static Statement stmt4 = new SimpleStatement("stmt4").setRoutingKey(key2);
  static Statement stmt5 = new SimpleStatement("stmt5").setRoutingKey(key3);
  static Statement stmt6 = new SimpleStatement("stmt6").setRoutingKey(key1);

  static BatchStatement batch12 = new BatchStatement(UNLOGGED).add(stmt1).add(stmt2);
  static BatchStatement batch126 = new BatchStatement(UNLOGGED).add(stmt1).add(stmt2).add(stmt6);
  static BatchStatement batch34 = new BatchStatement(UNLOGGED).add(stmt3).add(stmt4);

  @Test
  public void should_batch_by_routing_key_iterable() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements =
        batcher.batchByRoutingKey(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(statements).usingFieldByFieldElementComparator().contains(batch126, batch34, stmt5);
  }

  @Test
  public void should_batch_single_iterable() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    Statement statement = batcher.batchSingle(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
    assertThat(((BatchStatement) statement).getStatements())
        .containsExactly(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6);
  }

  @Test
  public void should_not_batch_one_statement_by_routing_key() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    List<Statement> statements = batcher.batchByRoutingKey(stmt1);
    assertThat(statements).containsOnly(stmt1);
  }

  @Test
  public void should_not_batch_one_statement_single() throws Exception {
    StatementBatcher batcher = new StatementBatcher();
    Statement statement = batcher.batchSingle(stmt1);
    assertThat(statement).isSameAs(stmt1);
  }
}
