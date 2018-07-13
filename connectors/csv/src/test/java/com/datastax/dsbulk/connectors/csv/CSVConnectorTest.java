/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.csv;

import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.readFile;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.HttpTestServer;
import com.datastax.dsbulk.commons.tests.logging.LogCapture;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.tests.logging.LogInterceptor;
import com.datastax.dsbulk.commons.tests.utils.FileUtils;
import com.datastax.dsbulk.commons.tests.utils.URLUtils;
import com.datastax.dsbulk.connectors.api.ErrorRecord;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.univocity.parsers.common.TextParsingException;
import io.undertow.util.Headers;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import reactor.core.publisher.Flux;

@ExtendWith(LogInterceptingExtension.class)
class CSVConnectorTest {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  private static final Config CONNECTOR_DEFAULT_SETTINGS =
      ConfigFactory.defaultReference().getConfig("dsbulk.connector.csv");

  @Test
  void should_read_single_file() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", comment = \"#\"", url("/sample.csv")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.defer(connector.read()).collectList().block();
    assertRecords(actual);
    connector.close();
  }

  @Test
  void should_read_single_file_by_resource() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", comment = \"#\"", url("/sample.csv")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.merge(connector.readByResource().get()).collectList().block();
    assertRecords(actual);
    connector.close();
  }

  private static void assertRecords(List<Record> actual) {
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).values())
        .containsExactly(
            "1997",
            "Ford",
            "E350",
            "  ac, abs, moon  ",
            "3000.00", // mapped
            "1997",
            "Ford",
            "E350",
            "  ac, abs, moon  ",
            "3000.00"); // indexed
    assertThat(actual.get(1).values())
        .containsExactly(
            "1999",
            "Chevy",
            "Venture \"Extended Edition\"",
            "",
            "4900.00",
            "1999",
            "Chevy",
            "Venture \"Extended Edition\"",
            "",
            "4900.00");
    assertThat(actual.get(2).values())
        .containsExactly(
            "1996",
            "Jeep",
            "Grand Cherokee",
            "MUST SELL!\nair, moon roof, loaded",
            "4799.00",
            "1996",
            "Jeep",
            "Grand Cherokee",
            "MUST SELL!\nair, moon roof, loaded",
            "4799.00");
    assertThat(actual.get(3).values())
        .containsExactly(
            "1999",
            "Chevy",
            "Venture \"Extended Edition, Very Large\"",
            "",
            "5000.00",
            "1999",
            "Chevy",
            "Venture \"Extended Edition, Very Large\"",
            "",
            "5000.00");
    assertThat(actual.get(4).values())
        .containsExactly(
            "",
            "",
            "Venture \"Extended Edition\"",
            "",
            "4900.00",
            "",
            "",
            "Venture \"Extended Edition\"",
            "",
            "4900.00");
  }

  @Test
  void should_read_from_stdin_with_special_encoding() throws Exception {
    InputStream stdin = System.in;
    try {
      String line = "fóô,bàr,qïx\n";
      InputStream is = new ByteArrayInputStream(line.getBytes("ISO-8859-1"));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("header = false, url = -, encoding = ISO-8859-1")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.defer(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(line);
      assertThat(actual.get(0).values()).containsExactly("fóô", "bàr", "qïx");
      connector.close();
    } finally {
      System.setIn(stdin);
    }
  }

  @Test
  void should_write_to_stdout_with_special_encoding() throws Exception {
    PrintStream stdout = System.out;
    try {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(baos);
      System.setOut(out);
      CSVConnector connector = new CSVConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("header = false, encoding = ISO-8859-1")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      Flux.<Record>just(new DefaultRecord(null, null, -1, null, "fóô", "bàr", "qïx"))
          .transform(connector.write())
          .blockLast();
      assertThat(new String(baos.toByteArray(), "ISO-8859-1"))
          .isEqualTo("fóô,bàr,qïx" + System.lineSeparator());

      connector.close();
    } finally {
      System.setOut(stdout);
    }
  }

  @Test
  void should_read_all_resources_in_directory() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("url = \"%s\", recursive = false", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_by_resource() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("url = \"%s\", recursive = false", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource().get()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_with_path() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Paths.get(getClass().getResource("/root").toURI());
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("url = \"%s\", recursive = false", escapeUserInput(rootPath)))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(String.format("url = \"%s\", recursive = true", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_recursively_by_resource() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(String.format("url = \"%s\", recursive = true", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.merge(connector.readByResource().get()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_scan_directory_recursively_with_custom_file_name_format() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", recursive = true, fileNamePattern = \"**/part-*\"",
                        url("/root-custom")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_warn_when_directory_empty(@LogCapture LogInterceptor logs) throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Files.createTempDirectory("empty");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format(
                          "url = \"%s\", recursive = true, fileNamePattern = \"**/part-*\"",
                          escapeUserInput(rootPath)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(String.format("Directory %s has no readable files.", rootPath));
      connector.close();
    } finally {
      deleteDirectory(rootPath);
    }
  }

  @Test
  void should_warn_when_no_files_matched(@LogCapture LogInterceptor logs) throws Exception {
    CSVConnector connector = new CSVConnector();
    Path rootPath = Files.createTempDirectory("empty");
    Files.createTempFile(rootPath, "test", ".txt");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format(
                          "url = \"%s\", recursive = true, fileNamePattern = \"**/part-*\"",
                          escapeUserInput(rootPath)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      assertThat(logs.getLoggedMessages())
          .contains(
              String.format(
                  "No files in directory %s matched the connector.csv.fileNamePattern of \"**/part-*\".",
                  rootPath));
      connector.close();
    } finally {
      deleteDirectory(rootPath);
    }
  }

  @Test
  void should_write_single_file() throws Exception {
    CSVConnector connector = new CSVConnector();
    // test directory creation
    Path dir = Files.createTempDirectory("test");
    Path out = dir.resolve("nonexistent");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format(
                          "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 1",
                          escapeUserInput(out)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
      assertThat(actual).hasSize(7);
      assertThat(actual)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "air, moon roof, loaded\",4799.00",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
    } finally {
      deleteDirectory(dir);
    }
  }

  @Test
  void should_write_multiple_files() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format(
                          "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 4",
                          escapeUserInput(out)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      // repeat the records 200 times to fully exercise multiple file writing
      Flux.fromIterable(createRecords()).repeat(200).transform(connector.write()).blockLast();
      connector.close();
      List<String> actual =
          FileUtils.readAllLinesInDirectoryAsStream(out)
              .sorted()
              .distinct()
              .collect(Collectors.toList());
      assertThat(actual)
          .containsExactly(
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              "Year,Make,Model,Description,Price",
              "air, moon roof, loaded\",4799.00");
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_roll_file_when_max_lines_reached() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format(
                          "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 1, maxRecords = 4",
                          escapeUserInput(out)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      Flux.fromIterable(createRecords()).transform(connector.write()).blockLast();
      connector.close();
      List<String> csv1 = Files.readAllLines(out.resolve("output-000001.csv"));
      List<String> csv2 = Files.readAllLines(out.resolve("output-000002.csv"));
      assertThat(csv1)
          .hasSize(5); // 1 header + 4 lines (3 records actually, but one record spans over 2 lines)
      assertThat(csv2).hasSize(3); // 1 header + 2 lines
      assertThat(csv1)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1997,Ford,E350,\"  ac, abs, moon  \",3000.00",
              "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
              "1996,Jeep,Grand Cherokee,\"MUST SELL!",
              "air, moon roof, loaded\",4799.00");
      assertThat(csv2)
          .containsExactly(
              "Year,Make,Model,Description,Price",
              "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
              ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_return_unmappable_record_when_line_malformed() throws Exception {
    InputStream stdin = System.in;
    try {
      String lines = "header1,header2\n" + "value1,value2,value3";
      InputStream is = new ByteArrayInputStream(lines.getBytes("UTF-8"));
      System.setIn(is);
      CSVConnector connector = new CSVConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("header = true").withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.defer(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0)).isInstanceOf(ErrorRecord.class);
      assertThat(actual.get(0).getSource()).isEqualTo("value1,value2,value3");
      assertThat(((ErrorRecord) actual.get(0)).getError())
          .isInstanceOf(IllegalArgumentException.class);
      assertThat(actual.get(0).values()).isEmpty();
      connector.close();
    } finally {
      System.setIn(stdin);
    }
  }

  @Test
  void should_skip_records() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("url = \"%s\", recursive = true, skipRecords = 10", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(450);
    connector.close();
  }

  @Test
  void should_skip_records2() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", recursive = true, skipRecords = 150", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(0);
    connector.close();
  }

  @Test
  void should_honor_max_records() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("url = \"%s\", recursive = true, maxRecords = 10", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(50);
    connector.close();
  }

  @Test
  void should_honor_max_records2() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format("url = \"%s\", recursive = true, maxRecords = 1", url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(5);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", recursive = true, skipRecords = 95, maxRecords = 10",
                        url("/root")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.defer(connector.read()).count().block()).isEqualTo(25);
    connector.close();
  }

  @Test
  void should_honor_max_records_and_skip_records2() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", skipRecords = 10, maxRecords = 1",
                        url("/root/ip-by-country-sample1.csv")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.defer(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getSource().toString().trim())
        .isEqualTo(
            "\"212.63.180.20\",\"212.63.180.23\",\"3560944660\",\"3560944663\",\"MZ\",\"Mozambique\"");
    connector.close();
  }

  @Test()
  void should_error_when_directory_is_not_empty() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      Path file = out.resolve("output-000001.csv");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format("url = \"%s\", maxConcurrentFiles = 1", escapeUserInput(out)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      assertThrows(IllegalArgumentException.class, connector::init);
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_abort_write_single_file_when_io_error() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format("url = \"%s\", maxConcurrentFiles = 1", escapeUserInput(out)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      Path file = out.resolve("output-000001.csv");
      // will cause the write to fail because the file already exists
      Files.createFile(file);
      assertThatThrownBy(
              () -> Flux.fromIterable(createRecords()).transform(connector.write()).blockLast())
          .hasRootCauseExactlyInstanceOf(FileAlreadyExistsException.class);
      connector.close();
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_abort_write_multiple_files_when_io_error() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    try {
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format("url = \"%s\", maxConcurrentFiles = 2", escapeUserInput(out)))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      Path file1 = out.resolve("output-000001.csv");
      Path file2 = out.resolve("output-000002.csv");
      // will cause the write workers to fail because the files already exist
      Files.createFile(file1);
      Files.createFile(file2);
      assertThatThrownBy(
              () ->
                  Flux.fromIterable(createRecords())
                      .repeat(100)
                      .transform(connector.write())
                      .blockLast())
          .hasRootCauseExactlyInstanceOf(FileAlreadyExistsException.class);
      connector.close();
    } finally {
      deleteDirectory(out);
    }
  }

  @Test
  void should_read_from_http_url() throws Exception {
    HttpTestServer server = new HttpTestServer();
    try {
      server.start(
          exchange -> {
            exchange.getResponseHeaders().add(Headers.CONTENT_TYPE, "text/csv");
            exchange.getResponseSender().send(readFile(path("/sample.csv")));
          });
      CSVConnector connector = new CSVConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString(
                      String.format(
                          "url = \"http://localhost:%d/file.csv\", escape = \"\\\"\", comment = \"#\"",
                          server.getPort()))
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.defer(connector.read()).collectList().block();
      assertRecords(actual);
      connector.close();
    } finally {
      server.stop();
    }
  }

  @Test
  void should_not_write_to_http_url() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("url = \"http://localhost:1234/file.csv\"")
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    assertThatThrownBy(
            () -> Flux.fromIterable(createRecords()).transform(connector.write()).blockLast())
        .isInstanceOf(UncheckedIOException.class)
        .hasMessageContaining(
            "HTTP/HTTPS protocols cannot be used for output: http://localhost:1234/file.csv");
    connector.close();
  }

  @Test
  void should_throw_IOE_when_max_chars_per_column_exceeded() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", comment = \"#\", maxCharsPerColumn = 15",
                        url("/sample.csv")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.defer(connector.read()).collectList().block())
        .satisfies(
            t ->
                assertThat(t.getCause())
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining(
                        "Length of parsed input (16) exceeds the maximum number "
                            + "of characters defined in your parser settings (15). "
                            + "Please increase the value of the connector.csv.maxCharsPerColumn setting.")
                    .hasCauseExactlyInstanceOf(TextParsingException.class)
                    .hasRootCauseExactlyInstanceOf(ArrayIndexOutOfBoundsException.class));
    connector.close();
  }

  @Test
  void should_throw_IOE_when_max_columns_exceeded() throws Exception {
    CSVConnector connector = new CSVConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", comment = \"#\", maxColumns = 1",
                        url("/sample.csv")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThatThrownBy(() -> Flux.defer(connector.read()).collectList().block())
        .satisfies(
            t ->
                assertThat(t)
                    .hasCauseInstanceOf(IOException.class)
                    .hasMessageContaining("Array index out of range: 1")
                    .hasMessageContaining(
                        "Please increase the value of the connector.csv.maxColumns "
                            + "or the connector.csv.maxCharsPerColumn setting.")
                    .hasRootCauseInstanceOf(ArrayIndexOutOfBoundsException.class));
    connector.close();
  }

  private static List<Record> createRecords() {
    ArrayList<Record> records = new ArrayList<>();
    String[] fields = new String[] {"Year", "Make", "Model", "Description", "Price"};
    records.add(
        new DefaultRecord(
            null, null, -1, null, fields, "1997", "Ford", "E350", "  ac, abs, moon  ", "3000.00"));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            fields,
            "1999",
            "Chevy",
            "Venture \"Extended Edition\"",
            null,
            "4900.00"));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            fields,
            "1996",
            "Jeep",
            "Grand Cherokee",
            "MUST SELL!\nair, moon roof, loaded",
            "4799.00"));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            fields,
            "1999",
            "Chevy",
            "Venture \"Extended Edition, Very Large\"",
            null,
            "5000.00"));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            fields,
            null,
            null,
            "Venture \"Extended Edition\"",
            null,
            "4900.00"));
    return records;
  }

  private static String url(String resource) {
    return CSVConnectorTest.class.getResource(resource).toExternalForm();
  }

  private static Path path(@SuppressWarnings("SameParameterValue") String resource)
      throws URISyntaxException {
    return Paths.get(CSVConnectorTest.class.getResource(resource).toURI());
  }
}
