/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import com.datastax.driver.core.Statement;
import java.io.Reader;
import org.reactivestreams.Publisher;

/**
 * A subclass of {@link CqlScriptReader} that exposes a {@link #readReactive() method} to read CQL
 * scripts in reactive mode.
 */
public abstract class AbstractReactiveCqlScriptReader extends CqlScriptReader {

  /**
   * Creates a new instance in single-line mode.
   *
   * @param in the script to read.
   */
  protected AbstractReactiveCqlScriptReader(Reader in) {
    super(in);
  }

  /**
   * Creates a new instance.
   *
   * @param in the script to read.
   * @param multiLine whether to use multi-line mode or not.
   */
  protected AbstractReactiveCqlScriptReader(Reader in, boolean multiLine) {
    super(in, multiLine);
  }

  /**
   * Creates a new instance.
   *
   * @param in the script to read.
   * @param multiLine whether to use multi-line mode or not.
   * @param size the size of the buffer.
   */
  protected AbstractReactiveCqlScriptReader(Reader in, boolean multiLine, int size) {
    super(in, multiLine, size);
  }

  /**
   * Reads the entire script and returns a {@link Publisher} of {@link Statement statement}s.
   *
   * @return a {@link Publisher} of {@link Statement statement}s.
   */
  public abstract Publisher<Statement> readReactive();
}
