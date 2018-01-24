/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;

/**
 * Thrown when a {@link StatementFormatter} encounters an error while formatting a {@link
 * Statement}.
 */
public class StatementFormatException extends RuntimeException {

  private final Statement statement;
  private final StatementFormatVerbosity verbosity;
  private final ProtocolVersion protocolVersion;
  private final CodecRegistry codecRegistry;

  public StatementFormatException(
      Statement statement,
      StatementFormatVerbosity verbosity,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry,
      Throwable t) {
    super(t);
    this.statement = statement;
    this.verbosity = verbosity;
    this.protocolVersion = protocolVersion;
    this.codecRegistry = codecRegistry;
  }

  /** @return The statement that failed to format. */
  public Statement getStatement() {
    return statement;
  }

  /** @return The requested verbosity. */
  public StatementFormatVerbosity getVerbosity() {
    return verbosity;
  }

  /** @return The protocol version in use. */
  public ProtocolVersion getProtocolVersion() {
    return protocolVersion;
  }

  /** @return The codec registry in use. */
  public CodecRegistry getCodecRegistry() {
    return codecRegistry;
  }
}
