/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import io.reactivex.Flowable;
import java.io.IOException;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.Test;

public abstract class AbstractReactiveCqlScriptReaderTest extends CqlScriptReaderTest {

  @Test
  public void should_publish_singleline_cql_script() throws Exception {
    AbstractReactiveCqlScriptReader reader = getCqlScriptReader("singleline.cql", false);
    List<Statement> statements =
        Flowable.fromPublisher(reader.readReactive()).toList().blockingGet();
    Assertions.assertThat(statements)
        .hasSize(5)
        .extracting("queryString")
        .containsExactly(singleline1, singleline2, singleline3, singleline4, singleline5);
  }

  @Test
  public void should_publish_multiline_cql_script() throws Exception {
    AbstractReactiveCqlScriptReader reader = getCqlScriptReader("multiline.cql", true);
    List<Statement> statements =
        Flowable.fromPublisher(reader.readReactive()).toList().blockingGet();
    Assertions.assertThat(statements).hasSize(5);
    Statement statement;
    statement = statements.get(0);
    assertThat(((BatchStatement) statement).getStatements())
        .extracting("queryString")
        .containsExactly(multiline1a, multiline1b);
    statement = statements.get(1);
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline2);
    statement = statements.get(2);
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline3);
    statement = statements.get(3);
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline4);
    statement = statements.get(4);
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline5);
  }

  @Test
  public void should_read_ddl_statements_reactive() throws Exception {
    AbstractReactiveCqlScriptReader reader = getCqlScriptReader("ddl.cql", true);
    List<Statement> statements =
        Flowable.fromPublisher(reader.readReactive()).toList().blockingGet();
    Assertions.assertThat(statements)
        .hasSize(3)
        .extracting("queryString")
        .containsExactly(ddl1, ddl2, ddl3);
  }

  @Override
  protected abstract AbstractReactiveCqlScriptReader getCqlScriptReader(
      String resource, boolean multiLine) throws IOException;
}
