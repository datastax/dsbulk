/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.statement;

import com.datastax.driver.core.Statement;
import io.reactivex.Flowable;
import java.util.List;
import org.junit.Test;

import static org.assertj.core.api.Assertions.assertThat;

/** */
public class RxJavaSortedStatementBatcherTest extends RxJavaStatementBatcherTest {

  @Test
  public void should_batch_as_operator() throws Exception {
    RxJavaSortedStatementBatcher batcher = new RxJavaSortedStatementBatcher();
    List<Statement> statements =
        Flowable.just(stmt1, stmt2, stmt3, stmt4, stmt5, stmt6)
            .compose(batcher)
            .toList()
            .blockingGet();
    assertThat(statements)
        .usingFieldByFieldElementComparator()
        .containsExactly(batch12, batch34, stmt5, stmt6);
  }
}
