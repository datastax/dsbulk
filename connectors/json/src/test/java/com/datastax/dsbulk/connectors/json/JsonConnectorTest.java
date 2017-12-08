/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.json;

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
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
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
@SuppressWarnings("Duplicates")
class JsonConnectorTest {

  static {
    URLUtils.setURLFactoryIfNeeded();
  }

  private static final Config CONNECTOR_DEFAULT_SETTINGS =
      ConfigFactory.defaultReference().getConfig("dsbulk.connector.json");

  private final JsonNodeFactory factory = JsonNodeFactory.instance;

  private final ObjectMapper objectMapper = new ObjectMapper();

  @Test
  void should_read_single_file_multi_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", parserFeatures = [ALLOW_COMMENTS]", url("/multi_doc.json")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    verifyRecords(actual);
    connector.close();
  }

  @Test
  void should_read_single_file_single_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", parserFeatures = [ALLOW_COMMENTS], mode = SINGLE_DOCUMENT",
                        url("/single_doc.json")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    verifyRecords(actual);
    connector.close();
  }

  @Test
  void should_read_single_empty_file_single_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", parserFeatures = [ALLOW_COMMENTS], mode = SINGLE_DOCUMENT",
                        url("/empty.json")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    // should complete with 0 records.
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertThat(actual).hasSize(0);
    connector.close();
  }

  @Test
  void should_read_single_empty_file_multi_doc() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", parserFeatures = [ALLOW_COMMENTS], mode = MULTI_DOCUMENT",
                        url("/empty.json")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    // should complete with 0 records.
    List<Record> actual = Flux.from(connector.read()).collectList().block();
    assertThat(actual).hasSize(0);
    connector.close();
  }

  @Test
  void should_read_single_file_by_resource() throws Exception {
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", parserFeatures = [ALLOW_COMMENTS]", url("/multi_doc.json")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> actual = Flux.merge(connector.readByResource()).collectList().block();
    verifyRecords(actual);
    connector.close();
  }

  @Test
  void should_read_from_stdin_with_special_encoding() throws Exception {
    InputStream stdin = System.in;
    try {
      String line = "{ \"fóô\" : \"bàr\", \"qïx\" : null }\n";
      InputStream is = new ByteArrayInputStream(line.getBytes("ISO-8859-1"));
      System.setIn(is);
      JsonConnector connector = new JsonConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("url = \"stdin:/\", encoding = ISO-8859-1")
                  .withFallback(CONNECTOR_DEFAULT_SETTINGS));
      connector.configure(settings, true);
      connector.init();
      assertThat(connector.isWriteToStandardOutput()).isFalse();
      List<Record> actual = Flux.from(connector.read()).collectList().block();
      assertThat(actual).hasSize(1);
      assertThat(actual.get(0).getSource()).isEqualTo(objectMapper.readTree(line));
      assertThat(actual.get(0).getFieldValue("fóô")).isEqualTo(factory.textNode("bàr"));
      assertThat(actual.get(0).getFieldValue("qïx")).isEqualTo(factory.nullNode());
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
      JsonConnector connector = new JsonConnector();
      LoaderConfig settings =
          new DefaultLoaderConfig(
              ConfigFactory.parseString("url = \"stdout:/\", encoding = ISO-8859-1")
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
          .isEqualTo("{\"0\":\"fóô\",\"1\":\"bàr\",\"2\":\"qïx\"}" + System.lineSeparator());
      connector.close();
    } finally {
      System.setOut(stdout);
      root.addAppender(stdoutAppender);
    }
  }

  @Test
  void should_read_all_resources_in_directory() throws Exception {
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
    String rootPath = JsonConnectorTest.class.getResource("/root").getPath();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    List<String> actual = Files.readAllLines(out.resolve("output-000001.json"));
    assertThat(actual).hasSize(5);
    assertThat(actual)
        .containsExactly(
            "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0}",
            "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
            "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0}",
            "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0}",
            "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}");
  }

  @Test
  void should_write_multiple_files() throws Exception {
    JsonConnector connector = new JsonConnector();
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
            "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0}",
            "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
            "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0}",
            "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0}",
            "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}");
  }

  @Test
  void should_roll_file_when_max_records_reached() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    String escapedPath = ConfigUtils.maybeEscapeBackslash(out.toString());
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", escape = \"\\\"\", maxConcurrentFiles = 1, maxRecords = 3",
                        escapedPath))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, false);
    connector.init();
    assertThat(connector.isWriteToStandardOutput()).isFalse();
    Flux<Record> records = Flux.fromIterable(createRecords()).publish().autoConnect(2);
    records.subscribe(connector.write());
    records.blockLast();
    connector.close();
    List<String> json1 = Files.readAllLines(out.resolve("output-000001.json"));
    List<String> json2 = Files.readAllLines(out.resolve("output-000002.json"));
    assertThat(json1).hasSize(3);
    assertThat(json2).hasSize(2);
    assertThat(json1)
        .containsExactly(
            "{\"Year\":1997,\"Make\":\"Ford\",\"Model\":\"E350\",\"Description\":\"ac, abs, moon\",\"Price\":3000.0}",
            "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}",
            "{\"Year\":1996,\"Make\":\"Jeep\",\"Model\":\"Grand Cherokee\",\"Description\":\"MUST SELL!\\nair, moon roof, loaded\",\"Price\":4799.0}");
    assertThat(json2)
        .containsExactly(
            "{\"Year\":1999,\"Make\":\"Chevy\",\"Model\":\"Venture \\\"Extended Edition, Very Large\\\"\",\"Description\":null,\"Price\":5000.0}",
            "{\"Year\":null,\"Make\":null,\"Model\":\"Venture \\\"Extended Edition\\\"\",\"Description\":null,\"Price\":4900.0}");
  }

  @Test
  void should_skip_records() throws Exception {
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
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
    JsonConnector connector = new JsonConnector();
    LoaderConfig settings =
        new DefaultLoaderConfig(
            ConfigFactory.parseString(
                    String.format(
                        "url = \"%s\", skipRecords = 10, maxRecords = 1",
                        url("/root/ip-by-country-sample1.json")))
                .withFallback(CONNECTOR_DEFAULT_SETTINGS));
    connector.configure(settings, true);
    connector.init();
    List<Record> records = Flux.from(connector.read()).collectList().block();
    assertThat(records).hasSize(1);
    assertThat(records.get(0).getSource().toString().trim())
        .isEqualTo(
            "{\"beginning IP Address\":\"212.63.180.20\",\"ending IP Address\":\"212.63.180.23\",\"beginning IP Number\":3560944660,\"ending IP Number\":3560944663,\"ISO 3166 Country Code\":\"MZ\",\"Country Name\":\"Mozambique\"}");
    connector.close();
  }

  @Test
  void should_cancel_write_single_file_when_io_error() throws Exception {
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    Path file = out.resolve("output-000001.json");
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
    JsonConnector connector = new JsonConnector();
    Path out = Files.createTempDirectory("test");
    Path file1 = out.resolve("output-000001.json");
    Path file2 = out.resolve("output-000002.json");
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

  private void verifyRecords(List<Record> actual) {
    assertThat(actual).hasSize(5);
    assertThat(actual.get(0).values())
        .containsOnly(
            factory.numberNode(1997),
            factory.textNode("Ford"),
            factory.textNode("E350"),
            factory.textNode("ac, abs, moon"),
            factory.numberNode(3000.00d));
    assertThat(actual.get(1).values())
        .containsOnly(
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition\""),
            factory.nullNode(),
            factory.numberNode(4900.00d));
    assertThat(actual.get(2).values())
        .containsOnly(
            factory.numberNode(1996),
            factory.textNode("Jeep"),
            factory.textNode("Grand Cherokee"),
            factory.textNode("MUST SELL!\nair, moon roof, loaded"),
            factory.numberNode(4799.00d));
    assertThat(actual.get(3).values())
        .containsOnly(
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition, Very Large\""),
            factory.nullNode(),
            factory.numberNode(5000.00d));
    assertThat(actual.get(4).values())
        .containsOnly(
            factory.nullNode(),
            factory.nullNode(),
            factory.textNode("Venture \"Extended Edition\""),
            factory.nullNode(),
            factory.numberNode(4900.00d));
  }

  private List<Record> createRecords() {
    ArrayList<Record> records = new ArrayList<>();
    String[] values = new String[] {"Year", "Make", "Model", "Description", "Price"};
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            values,
            factory.numberNode(1997),
            factory.textNode("Ford"),
            factory.textNode("E350"),
            factory.textNode("ac, abs, moon"),
            factory.numberNode(3000.00d)));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            values,
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition\""),
            null,
            factory.numberNode(4900.00d)));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            values,
            factory.numberNode(1996),
            factory.textNode("Jeep"),
            factory.textNode("Grand Cherokee"),
            factory.textNode("MUST SELL!\nair, moon roof, loaded"),
            factory.numberNode(4799.00d)));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            values,
            factory.numberNode(1999),
            factory.textNode("Chevy"),
            factory.textNode("Venture \"Extended Edition, Very Large\""),
            null,
            factory.numberNode(5000.00d)));
    records.add(
        new DefaultRecord(
            null,
            null,
            -1,
            null,
            values,
            factory.nullNode(),
            factory.nullNode(),
            factory.textNode("Venture \"Extended Edition\""),
            factory.nullNode(),
            factory.numberNode(4900.00d)));
    return records;
  }

  private static String url(String resource) {
    return JsonConnectorTest.class.getResource(resource).toExternalForm();
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
