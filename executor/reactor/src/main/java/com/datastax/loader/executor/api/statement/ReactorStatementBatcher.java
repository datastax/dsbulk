/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.statement;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/** A subclass of {@link StatementBatcher} that adds reactive-style capabilities to it. */
public class ReactorStatementBatcher extends StatementBatcher {

  /**
   * Creates a new {@link StatementBatcher} that produces {@link BatchStatement.Type#UNLOGGED
   * unlogged} batches, and uses the {@link ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance.
   */
  public ReactorStatementBatcher() {}

  /**
   * Creates a new {@link StatementBatcher} that produces {@link BatchStatement.Type#UNLOGGED
   * unlogged} batches, and uses the given {@link Cluster} as its source for the {@link
   * ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   *
   * @param cluster The {@link Cluster} to use.
   */
  public ReactorStatementBatcher(Cluster cluster) {
    super(cluster);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * and uses the given {@code protocolVersion} and the given {@code codecRegistry}.
   *
   * @param batchType The {@link BatchStatement.Type batch type} to use.
   * @param protocolVersion The {@link ProtocolVersion} to use.
   * @param codecRegistry The {@link CodecRegistry} to use.
   */
  public ReactorStatementBatcher(
      BatchStatement.Type batchType, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    super(batchType, protocolVersion, codecRegistry);
  }

  /**
   * Batches together the given statements into groups of statements having the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}.
   *
   * @param statements the statements to batch together.
   * @return A publisher of batched statements.
   */
  public Flux<Statement> batchByRoutingKey(Publisher<? extends Statement> statements) {
    return Flux.from(statements).groupBy(this::routingKey).flatMap(this::batchSingle);
  }

  /**
   * Batches together all the given statements into one single {@link BatchStatement}. The returns
   * {@link Publisher} is guaranteed to only emit on single item.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A publisher of one single {@link BatchStatement} containing all the given statements
   *     batched together.
   */
  public Mono<Statement> batchSingle(Publisher<? extends Statement> statements) {
    return Flux.from(statements)
        .reduce(new BatchStatement(batchType), BatchStatement::add)
        // Don't wrap single statements in batch.
        .map(batch -> batch.size() == 1 ? batch.getStatements().iterator().next() : batch);
  }
}
