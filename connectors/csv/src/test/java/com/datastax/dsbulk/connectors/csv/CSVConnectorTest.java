/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.csv;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.commons.internal.utils.URLUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.UnmappableRecord;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;

/** */
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
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).values())
        .containsOnly("1997", "Ford", "E350", "ac, abs, moon", "3000.00");
    assertThat(actual.get(1).values())
        .containsOnly("1999", "Chevy", "Venture \"Extended Edition\"", null, "4900.00");
    assertThat(actual.get(2).values())
        .containsOnly(
            "1996", "Jeep", "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded", "4799.00");
    assertThat(actual.get(3).values())
        .containsOnly("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", null, "5000.00");
    assertThat(actual.get(4).values())
        .containsOnly(null, null, "Venture \"Extended Edition\"", null, "4900.00");
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
    List<Record> actual = Flux.merge(connector.readByResource()).collectList().block();
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).values())
        .containsOnly("1997", "Ford", "E350", "ac, abs, moon", "3000.00");
    assertThat(actual.get(1).values())
        .containsOnly("1999", "Chevy", "Venture \"Extended Edition\"", null, "4900.00");
    assertThat(actual.get(2).values())
        .containsOnly(
            "1996", "Jeep", "Grand Cherokee", "MUST SELL!\nair, moon roof, loaded", "4799.00");
    assertThat(actual.get(3).values())
        .containsOnly("1999", "Chevy", "Venture \"Extended Edition, Very Large\"", null, "5000.00");
    assertThat(actual.get(4).values())
        .containsOnly(null, null, "Venture \"Extended Edition\"", null, "4900.00");
    connector.close();
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
              ConfigFactory.parseString("header = false, url = \"stdin:/\", encoding = ISO-8859-1")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      assertThat(connector.isWriteToStandardOutput()).isFalse();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
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
    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    Appender<ILoggingEvent> stdoutAppender = root.getAppender("STDOUT");
    try {
      root.detachAppender(stdoutAppender);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream out = new PrintStream(baos);
      System.setOut(out);
      CSVConnector connector = new CSVConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("header = false, url = \"stdout:/\", encoding = ISO-8859-1")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, false);
      connector.init();
      assertThat(connector.isWriteToStandardOutput()).isTrue();
      Flux<Record> records =
          Flux.<Record>just(new DefaultRecord(null, null, -1, null, "fóô", "bàr", "qïx"))
              .publish()
              .autoConnect(2);
      records.subscribe(connector.write());
      records.blockLast();
      assertThat(new String(baos.toByteArray(), "ISO-8859-1"))
          .isEqualTo("fóô,bàr,qïx" + System.lineSeparator());

      connector.close();
    } finally {
      System.setOut(stdout);
      root.addAppender(stdoutAppender);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(300);
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
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(300);
    connector.close();
  }

  @Test
  void should_read_all_resources_in_directory_with_path() throws Exception {
    CSVConnector connector = new CSVConnector();
    String rootPath = CSVConnectorTest.class.getResource("/root").getPath();
    if (PlatformUtils.isWindows()) {
      // Root-path is of the form "/C:/foo/bar/root". We want forward
      // slashes, but we don't want the leading slash.
      rootPath = rootPath.substring(1);
    }
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(String.format("url = \"%s\", recursive = false", rootPath))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(300);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(500);
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
    assertThat(Flux.merge(connector.readByResource()).count().block()).isEqualTo(500);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(500);
    connector.close();
  }

  @Test
  void should_write_single_file() throws Exception {
    CSVConnector connector = new CSVConnector();
    // test directory creation
    Path out = Files.createTempDirectory("test").resolve("nonexistent");
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 1",
                        ConfigUtils.maybeEscapeBackslash(out.toString())))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    assertThat(connector.isWriteToStandardOutput()).isFalse();
    Flux<Record> records = Flux.fromIterable(createRecords()).publish().autoConnect(2);
    records.subscribe(connector.write());
    records.blockLast();
    connector.close();
    List<String> actual = Files.readAllLines(out.resolve("output-000001.csv"));
    assertThat(actual).hasSize(7);
    assertThat(actual)
        .containsExactly(
            "Year,Make,Model,Description,Price",
            "1997,Ford,E350,\"ac, abs, moon\",3000.00",
            "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
            "1996,Jeep,Grand Cherokee,\"MUST SELL!",
            "air, moon roof, loaded\",4799.00",
            "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
            ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
  }

  @Test
  void should_write_multiple_files() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 4",
                        ConfigUtils.maybeEscapeBackslash(out.toString())))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    assertThat(connector.isWriteToStandardOutput()).isFalse();
    Flux<Record> records = Flux.fromIterable(createRecords()).publish().autoConnect(2);
    records.subscribe(connector.write());
    records.blockLast();
    connector.close();
    List<String> actual = collectLines(out);
    assertThat(actual)
        .containsOnly(
            "Year,Make,Model,Description,Price",
            "1997,Ford,E350,\"ac, abs, moon\",3000.00",
            "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
            "1996,Jeep,Grand Cherokee,\"MUST SELL!",
            "air, moon roof, loaded\",4799.00",
            "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
            ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
  }

  @Test
  void should_roll_file_when_max_lines_reached() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 1, maxRecords = 4",
                        ConfigUtils.maybeEscapeBackslash(out.toString())))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    assertThat(connector.isWriteToStandardOutput()).isFalse();
    Flux<Record> records = Flux.fromIterable(createRecords()).publish().autoConnect(2);
    records.subscribe(connector.write());
    records.blockLast();
    connector.close();
    List<String> csv1 = Files.readAllLines(out.resolve("output-000001.csv"));
    List<String> csv2 = Files.readAllLines(out.resolve("output-000002.csv"));
    assertThat(csv1)
        .hasSize(5); // 1 header + 4 lines (3 records actually, but one record spans over 2 lines)
    assertThat(csv2).hasSize(3); // 1 header + 2 lines
    assertThat(csv1)
        .containsOnly(
            "Year,Make,Model,Description,Price",
            "1997,Ford,E350,\"ac, abs, moon\",3000.00",
            "1999,Chevy,\"Venture \"\"Extended Edition\"\"\",,4900.00",
            "1996,Jeep,Grand Cherokee,\"MUST SELL!",
            "air, moon roof, loaded\",4799.00");
    assertThat(csv2)
        .containsOnly(
            "Year,Make,Model,Description,Price",
            "1999,Chevy,\"Venture \"\"Extended Edition, Very Large\"\"\",,5000.00",
            ",,\"Venture \"\"Extended Edition\"\"\",,4900.00");
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
              ConfigFactory.parseString("header = true, url = \"stdin:/\"")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0)).isInstanceOf(UnmappableRecord.class);
      assertThat(actual.get(0).getSource()).isEqualTo("value1,value2,value3");
      assertThat(((UnmappableRecord) actual.get(0)).getError())
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(450);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(0);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(50);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(5);
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
    assertThat(Flux.from(connector.read()).count().block()).isEqualTo(25);
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
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getSource().toString().trim())
        .isEqualTo(
            "\"212.63.180.20\",\"212.63.180.23\",\"3560944660\",\"3560944663\",\"MZ\",\"Mozambique\"");
    connector.close();
  }

  @Test
  void should_cancel_write_single_file_when_io_error() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    Path file = out.resolve("output-000001.csv");
    // will cause the write to fail because the file already exists
    Files.createFile(file);
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", maxConcurrentFiles = 1",
                        ConfigUtils.maybeEscapeBackslash(out.toString())))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    Flux<Record> records = Flux.fromIterable(createRecords());
    CountDownLatch latch = new CountDownLatch(1);
    records.doOnCancel(latch::countDown).subscribe(connector.write());
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    connector.close();
  }

  @Test
  void should_cancel_write_multiple_files_when_io_error() throws Exception {
    CSVConnector connector = new CSVConnector();
    Path out = Files.createTempDirectory("test");
    Path file1 = out.resolve("output-000001.csv");
    Path file2 = out.resolve("output-000002.csv");
    // will cause the write workers to fail because the files already exist
    Files.createFile(file1);
    Files.createFile(file2);
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", maxConcurrentFiles = 2",
                        ConfigUtils.maybeEscapeBackslash(out.toString())))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    UnicastProcessor<Record> records = UnicastProcessor.create();
    CountDownLatch latch = new CountDownLatch(1);
    records.doOnCancel(latch::countDown).subscribe(connector.write());
    List<Record> list = createRecords();
    records.onNext(list.get(0));
    // don't send a terminal event as the terminal event could cancel the upstream subscription
    // before the write workers are created and fail to write.
    assertThat(latch.await(5, TimeUnit.SECONDS)).isTrue();
    connector.close();
  }

  private static List<Record> createRecords() {
    ArrayList<Record> records = new ArrayList<>();
    String[] fields = new String[] {"Year", "Make", "Model", "Description", "Price"};
    records.add(
        new DefaultRecord(
            null, null, -1, null, fields, "1997", "Ford", "E350", "ac, abs, moon", "3000.00"));
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

  private static List<String> collectLines(Path out) throws IOException {
    try (Stream<Path> paths = Files.walk(out)) {
      return paths
          .filter(Files::isRegularFile)
          .flatMap(
              path -> {
                try {
                  return Files.readAllLines(path).stream();
                } catch (IOException e) {
                  throw new UncheckedIOException(e);
                }
              })
          .collect(Collectors.toList());
    }
  }
}
