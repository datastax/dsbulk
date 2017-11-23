/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.cql;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.google.common.io.Resources;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.BeforeClass;
import org.junit.Test;

public class CqlScriptReaderTest {

  static String multiline1a;
  static String multiline1b;
  static String multiline2;
  static String multiline3;
  static String multiline4;
  static String multiline5;

  static String singleline1;
  static String singleline2;
  static String singleline3;
  static String singleline4;
  static String singleline5;

  static String ddl1;
  static String ddl2;
  static String ddl3;

  @BeforeClass
  public static void readStatements() throws Exception {
    multiline1a = readFile("multiline1a.cql");
    multiline1b = readFile("multiline1b.cql");
    multiline2 = readFile("multiline2.cql");
    multiline3 = readFile("multiline3.cql");
    multiline4 = readFile("multiline4.cql");
    multiline5 = readFile("multiline5.cql");
    singleline1 = readFile("singleline1.cql");
    singleline2 = readFile("singleline2.cql");
    singleline3 = readFile("singleline3.cql");
    singleline4 = readFile("singleline4.cql");
    singleline5 = readFile("singleline5.cql");
    ddl1 = readFile("ddl1.cql");
    ddl2 = readFile("ddl2.cql");
    ddl3 = readFile("ddl3.cql");
  }

  @Test
  public void should_read_singleline_cql_script() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("singleline.cql", false);
    Statement statement;
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(singleline1);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(singleline2);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(singleline3);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(singleline4);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(singleline5);
    assertThat(reader.readStatement()).isNull();
  }

  @Test
  public void should_stream_singleline_cql_script() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("singleline.cql", false);
    List<Statement> statements = reader.readStream().collect(toList());
    assertThat(statements)
        .hasSize(5)
        .extracting("queryString")
        .containsExactly(singleline1, singleline2, singleline3, singleline4, singleline5);
  }

  @Test
  public void should_read_multiline_cql_script() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("multiline.cql", true);
    Statement statement;
    statement = reader.readStatement();
    assertThat(((BatchStatement) statement).getStatements())
        .extracting("queryString")
        .containsExactly(multiline1a, multiline1b);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline2);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline3);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline4);
    statement = reader.readStatement();
    assertThat(((RegularStatement) statement).getQueryString()).isEqualTo(multiline5);
    assertThat(reader.readStatement()).isNull();
  }

  @Test
  public void should_stream_multiline_cql_script() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("multiline.cql", true);
    List<Statement> statements = reader.readStream().collect(toList());
    assertThat(statements).hasSize(5);
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
  public void should_read_ddl_statements() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("ddl.cql", true);
    List<Statement> statements = reader.readStream().collect(toList());
    Assertions.assertThat(statements)
        .hasSize(3)
        .extracting("queryString")
        .containsExactly(ddl1, ddl2, ddl3);
  }

  @Test
  public void should_read_singleline_cql_script_2() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("ip-by-country-sample.cql", false);
    List<Statement> statements = reader.readStream().collect(toList());
    Assertions.assertThat(statements).hasSize(500);
  }

  @Test
  public void should_read_multiline_cql_script_2() throws Exception {
    CqlScriptReader reader = getCqlScriptReader("ip-by-country-sample.cql", true);
    List<Statement> statements = reader.readStream().collect(toList());
    Assertions.assertThat(statements).hasSize(500);
  }

  protected CqlScriptReader getCqlScriptReader(String resource, boolean multiLine)
      throws IOException {
    URL url = Resources.getResource(resource);
    return new CqlScriptReader(Resources.asCharSource(url, UTF_8).openBufferedStream(), multiLine);
  }

  private static String readFile(String resource) throws IOException {
    URL url = Resources.getResource(resource);

    if (PlatformUtils.isWindows()) {
      return Resources.toString(url, UTF_8).trim().replace("\r\n", "\n");
    }
    return Resources.toString(url, UTF_8).trim();
  }
}
