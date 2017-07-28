/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.cql;

import static reactor.core.publisher.FluxSink.OverflowStrategy.BUFFER;

import com.datastax.driver.core.Statement;
import java.io.IOException;
import java.io.Reader;
import reactor.core.publisher.Flux;

/**
 * A {@link CqlScriptReader} that exposes <a href="https://projectreactor.io">Reactor</a> types for
 * easy consumption by clients using this library.
 */
public class ReactorCqlScriptReader extends AbstractReactiveCqlScriptReader {

  /**
   * Creates a new instance in single-line mode.
   *
   * @param in the script to read.
   */
  public ReactorCqlScriptReader(Reader in) {
    super(in);
  }

  /**
   * Creates a new instance.
   *
   * @param in the script to read.
   * @param multiLine whether to use multi-line mode or not.
   */
  public ReactorCqlScriptReader(Reader in, boolean multiLine) {
    super(in, multiLine);
  }

  /**
   * Creates a new instance.
   *
   * @param in the script to read.
   * @param multiLine whether to use multi-line mode or not.
   * @param size the size of the buffer.
   */
  public ReactorCqlScriptReader(Reader in, boolean multiLine, int size) {
    super(in, multiLine, size);
  }

  @Override
  public Flux<Statement> readReactive() {
    return Flux.create(
        e -> {
          Statement nextStatement;
          try {
            while ((nextStatement = readStatement()) != null) {
              e.next(nextStatement);
            }
            e.complete();
          } catch (IOException ex) {
            e.error(ex);
          }
        },
        BUFFER);
  }
}
