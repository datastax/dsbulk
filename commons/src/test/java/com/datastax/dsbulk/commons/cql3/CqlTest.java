/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.cql3;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.junit.jupiter.api.Test;

class CqlTest {

  @Test
  void should_parse_select_statement() {
    ParseTree query =
        parse(
            "SELECT col1, col2 AS alias2 FROM ks1.table1 WHERE col1 = 123 AND col2 = 'abc' ORDER BY col1 ASC, col2 DESC");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementSelectStatementContext.class);
  }

  @Test
  void should_parse_insert_statement() {
    ParseTree query =
        parse(
            "INSERT INTO ks1.table1 (col1, col2) VALUES (123, 'abc') IF NOT EXISTS USING TTL 123 AND TIMESTAMP 123");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementInsertStatementContext.class);
  }

  @Test
  void should_parse_update_statement() {
    ParseTree query =
        parse(
            "UPDATE ks1.table1 USING TTL 123 AND TIMESTAMP 123 SET col1 = 123, col2 = 'abc' WHERE col3 = [1,2] IF col4 = {1,2}");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementUpdateStatementContext.class);
  }

  @Test
  void should_parse_delete_statement() {
    ParseTree query =
        parse(
            "DELETE col1, col2 FROM ks1.table1 USING TIMESTAMP 123 WHERE col1 = 123 AND col2 = 'abc' IF EXISTS");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementDeleteStatementContext.class);
  }

  @Test
  void should_parse_batch_statement() {
    ParseTree query =
        parse(
            "BEGIN UNLOGGED BATCH USING TTL 123 AND TIMESTAMP 123 "
                + "UPDATE ks1.table1 USING TTL 123 AND TIMESTAMP 123 SET col1 = 123, col2 = 'abc' WHERE col3 = [1,2] IF col4 = {1,2};"
                + "INSERT INTO ks1.table1 (col1, col2) VALUES (123, 'abc') IF NOT EXISTS USING TTL 123 AND TIMESTAMP 123 "
                + "APPLY BATCH");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementBatchStatementContext.class);
  }

  private static ParseTree parse(String query) {
    CodePointCharStream input = CharStreams.fromString(query);
    CqlLexer lexer = new CqlLexer(input);
    CommonTokenStream tokens = new CommonTokenStream(lexer);
    CqlParser parser = new CqlParser(tokens);
    parser.removeErrorListeners();
    parser.addErrorListener(
        new BaseErrorListener() {
          @Override
          public void syntaxError(
              Recognizer<?, ?> recognizer,
              Object offendingSymbol,
              int line,
              int charPositionInLine,
              String msg,
              RecognitionException e) {
            fail(msg, e);
          }
        });
    return parser.cqlStatement();
  }
}
