/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log.statement;

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
