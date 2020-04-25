/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.generated.cql3;

import static org.assertj.core.api.Assertions.assertThat;

import org.antlr.v4.runtime.BaseErrorListener;
import org.antlr.v4.runtime.CharStreams;
import org.antlr.v4.runtime.CodePointCharStream;
import org.antlr.v4.runtime.CommonTokenStream;
import org.antlr.v4.runtime.RecognitionException;
import org.antlr.v4.runtime.Recognizer;
import org.antlr.v4.runtime.tree.ParseTree;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

class CqlParserSmokeTest {

  @Test
  void should_parse_select_statement() {
    ParseTree query =
        parse(
            "SELECT col1, col2 AS alias2 FROM ks1.table1 WHERE col1 = 123 AND col2 = 'abc' ORDER BY col1 ASC, col2 DESC");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementContext.class);
  }

  @Test
  void should_parse_insert_statement() {
    ParseTree query =
        parse(
            "INSERT INTO ks1.table1 (col1, col2) VALUES (123, 'abc') IF NOT EXISTS USING TTL 123 AND TIMESTAMP 123");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementContext.class);
  }

  @Test
  void should_parse_update_statement() {
    ParseTree query =
        parse(
            "UPDATE ks1.table1 USING TTL 123 AND TIMESTAMP 123 SET col1 = 123, col2 = 'abc' WHERE col3 = [1,2] IF col4 = {1,2}");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementContext.class);
  }

  @Test
  void should_parse_delete_statement() {
    ParseTree query =
        parse(
            "DELETE col1, col2 FROM ks1.table1 USING TIMESTAMP 123 WHERE col1 = 123 AND col2 = 'abc' IF EXISTS");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementContext.class);
  }

  @Test
  void should_parse_batch_statement() {
    ParseTree query =
        parse(
            "BEGIN UNLOGGED BATCH USING TTL 123 AND TIMESTAMP 123 "
                + "UPDATE ks1.table1 USING TTL 123 AND TIMESTAMP 123 SET col1 = 123, col2 = 'abc' WHERE col3 = [1,2] IF col4 = {1,2};"
                + "INSERT INTO ks1.table1 (col1, col2) VALUES (123, 'abc') IF NOT EXISTS USING TTL 123 AND TIMESTAMP 123 "
                + "APPLY BATCH");
    assertThat(query).isInstanceOf(CqlParser.CqlStatementContext.class);
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
            Assertions.fail(msg, e);
          }
        });
    return parser.cqlStatement();
  }
}
