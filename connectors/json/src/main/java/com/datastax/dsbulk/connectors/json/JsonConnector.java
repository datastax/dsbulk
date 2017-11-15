/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.json;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.commons.url.LoaderURLStreamHandlerFactory;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.json.internal.SchemaFreeJsonRecordMetadata;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.typesafe.config.ConfigException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.WorkQueueProcessor;

/**
 * A connector for Json files.
 *
 * <p>It is capable of reading from any URL, provided that there is a {@link URLStreamHandler
 * handler} installed for it. For file URLs, it is also capable of reading several files at once
 * from a given root directory.
 *
 * <p>This connector is highly configurable; see its {@code reference.conf} file, bundled within its
 * jar archive, for detailed information.
 */
public class JsonConnector implements Connector {

  enum DocumentMode {
    MULTI_DOCUMENT,
    SINGLE_DOCUMENT
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonConnector.class);

  private static final String URL = "url";
  private static final String MODE = "mode";
  private static final String FILE_NAME_PATTERN = "fileNamePattern";
  private static final String ENCODING = "encoding";
  private static final String SKIP_RECORDS = "skipRecords";
  private static final String MAX_RECORDS = "maxRecords";
  private static final String MAX_CONCURRENT_FILES = "maxConcurrentFiles";
  private static final String RECURSIVE = "recursive";
  private static final String FILE_NAME_FORMAT = "fileNameFormat";
  private static final String PARSER_FEATURES = "parserFeatures";
  private static final String GENERATOR_FEATURES = "generatorFeatures";
  private static final String PRETTY_PRINT = "prettyPrint";

  private static final TypeReference<Map<String, JsonNode>> JSON_NODE_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, JsonNode>>() {};

  private boolean read;
  private URL url;
  private DocumentMode mode;
  private Path root;
  private String pattern;
  private Charset encoding;
  private long skipRecords;
  private long maxRecords;
  private int maxConcurrentFiles;
  private boolean recursive;
  private String fileNameFormat;
  private int resourceCount;
  private AtomicInteger counter;
  private ExecutorService threadPool;
  private WorkQueueProcessor<Record> writeQueueProcessor;
  private ObjectMapper objectMapper;
  private JavaType jsonNodeMapType;
  private List<JsonParser.Feature> parserFeatures;
  private List<JsonGenerator.Feature> generatorFeatures;
  private String lineSeparator;
  private boolean prettyPrint;

  @Override
  public RecordMetadata getRecordMetadata() {
    return new SchemaFreeJsonRecordMetadata();
  }

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    try {
      if (!settings.hasPath("url")) {
        throw new BulkConfigurationException(
            "url is mandatory when using the json connector. Please set connector.json.url "
                + "and try again. See settings.md or help for more information.",
            "connector.json");
      }
      this.read = read;
      url = settings.getURL(URL);
      mode = settings.getEnum(DocumentMode.class, MODE);
      pattern = settings.getString(FILE_NAME_PATTERN);
      encoding = settings.getCharset(ENCODING);
      skipRecords = settings.getLong(SKIP_RECORDS);
      maxRecords = settings.getLong(MAX_RECORDS);
      maxConcurrentFiles = settings.getThreads(MAX_CONCURRENT_FILES);
      recursive = settings.getBoolean(RECURSIVE);
      fileNameFormat = settings.getString(FILE_NAME_FORMAT);
      parserFeatures = settings.getEnumList(JsonParser.Feature.class, PARSER_FEATURES);
      generatorFeatures = settings.getEnumList(JsonGenerator.Feature.class, GENERATOR_FEATURES);
      prettyPrint = settings.getBoolean(PRETTY_PRINT);
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.json");
    }
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    if (read) {
      tryReadFromDirectory();
    } else {
      tryWriteToDirectory();
    }
    objectMapper = new ObjectMapper();
    jsonNodeMapType = objectMapper.constructType(JSON_NODE_MAP_TYPE_REFERENCE.getType());
    if (read) {
      for (JsonParser.Feature parserFeature : parserFeatures) {
        objectMapper.configure(parserFeature, true);
      }
    } else {
      for (JsonGenerator.Feature generatorFeature : generatorFeatures) {
        objectMapper.configure(generatorFeature, true);
      }
      lineSeparator = System.getProperty("line.separator");
      counter = new AtomicInteger(0);
      if (prettyPrint) {
        objectMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter(lineSeparator));
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (writeQueueProcessor != null) {
      writeQueueProcessor.awaitAndShutdown(5, SECONDS);
    }
    if (threadPool != null) {
      threadPool.shutdown();
      threadPool.awaitTermination(5, SECONDS);
      threadPool.shutdownNow();
    }
  }

  @Override
  public int estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public boolean isWriteToStandardOutput() {
    return url.getProtocol().equalsIgnoreCase(LoaderURLStreamHandlerFactory.STDOUT);
  }

  @Override
  public Publisher<Record> read() {
    assert read;
    if (root != null) {
      return scanRootDirectory().flatMap(this::readURL);
    } else {
      return readURL(url);
    }
  }

  @Override
  public Publisher<Publisher<Record>> readByResource() {
    if (root != null) {
      return scanRootDirectory().map(this::readURL);
    } else {
      return Flux.just(readURL(url));
    }
  }

  @Override
  public Subscriber<Record> write() {
    assert !read;
    if (root != null && maxConcurrentFiles > 1) {
      return multipleWriteSubscriber();
    } else {
      return singleWriteSubscriber();
    }
  }

  private void tryReadFromDirectory() throws URISyntaxException {
    try {
      resourceCount = 1;
      Path root = Paths.get(url.toURI());
      if (Files.isDirectory(root)) {
        if (!Files.isReadable(root)) {
          throw new IllegalArgumentException("Directory is not readable: " + root);
        }
        this.root = root;
        resourceCount = scanRootDirectory().take(100).count().block().intValue();
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to reading from URL directly
    }
  }

  private void tryWriteToDirectory() throws URISyntaxException, IOException {
    try {
      resourceCount = -1;
      Path root = Paths.get(url.toURI());
      if (!Files.exists(root)) {
        root = Files.createDirectories(root);
      }
      if (Files.isDirectory(root)) {
        if (!Files.isWritable(root)) {
          throw new IllegalArgumentException("Directory is not writable: " + root);
        }
        this.root = root;
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem, fall back to writing to URL directly
    }
  }

  private Flux<Record> readURL(URL url) {
    Flux<Record> records =
        Flux.create(
            sink -> {
              LOGGER.debug("Reading {}", url);
              URI resource = URIUtils.createResourceURI(url);
              SimpleBackpressureController controller = new SimpleBackpressureController();
              sink.onRequest(controller::signalRequested);
              JsonFactory factory = objectMapper.getFactory();
              try (BufferedReader r = IOUtils.newBufferedReader(url, encoding);
                  JsonParser parser = factory.createParser(r)) {
                sink.onDispose(
                    () -> {
                      try {
                        parser.close();
                      } catch (IOException e) {
                        LOGGER.error("Could not close Json parser: " + e.getMessage(), e);
                      }
                    });
                if (mode == DocumentMode.SINGLE_DOCUMENT) {
                  while (parser.currentToken() != JsonToken.START_ARRAY) {
                    parser.nextToken();
                  }
                  parser.nextToken();
                }
                MappingIterator<JsonNode> it = objectMapper.readValues(parser, JsonNode.class);
                long recordNumber = 1;
                while (!sink.isCancelled() && it.hasNext()) {
                  long finalRecordNumber = recordNumber++;
                  Supplier<URI> location =
                      Suppliers.memoize(() -> URIUtils.createLocationURI(url, finalRecordNumber));
                  Record record;
                  JsonNode node = it.next();
                  Map<String, JsonNode> values = objectMapper.convertValue(node, jsonNodeMapType);
                  record =
                      new DefaultRecord(node, () -> resource, finalRecordNumber, location, values);
                  LOGGER.trace("Emitting record {}", record);
                  controller.awaitRequested(1);
                  sink.next(record);
                }
                LOGGER.debug("Done reading {}", url);
                sink.complete();
              } catch (Exception e) {
                LOGGER.error(String.format("Error reading from %s: %s", url, e.getMessage()), e);
                sink.error(e);
              }
            },
            FluxSink.OverflowStrategy.ERROR);
    if (skipRecords > 0) {
      records = records.skip(skipRecords);
    }
    if (maxRecords != -1) {
      records = records.take(maxRecords);
    }
    return records;
  }

  private Flux<URL> scanRootDirectory() {
    PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
    return Flux.defer(
            () -> {
              try {
                // this stream will be closed by the flux, do not add it to a try-with-resources block
                Stream<Path> files = Files.walk(root, recursive ? Integer.MAX_VALUE : 1);
                return Flux.fromStream(files);
              } catch (IOException e) {
                throw new RuntimeException(e);
              }
            })
        .filter(Files::isReadable)
        .filter(Files::isRegularFile)
        .filter(matcher::matches)
        .map(
            file -> {
              try {
                return file.toUri().toURL();
              } catch (MalformedURLException e) {
                throw new RuntimeException(e);
              }
            });
  }

  @NotNull
  private Subscriber<Record> singleWriteSubscriber() {

    return new BaseSubscriber<Record>() {

      private URL url;
      private JsonGenerator writer;
      private long currentLine;

      private void start() {
        url = getOrCreateDestinationURL();
        writer = createJsonWriter(url);
        currentLine = 0;
        LOGGER.debug("Writing " + url);
      }

      @Override
      protected void hookOnSubscribe(Subscription subscription) {
        start();
        subscription.request(Long.MAX_VALUE);
      }

      @Override
      protected void hookOnNext(Record record) {
        if (root != null && currentLine == maxRecords) {
          end(SignalType.ON_COMPLETE);
          start();
        }
        LOGGER.trace("Writing record {}", record);
        try {
          writer.writeStartObject();
          for (String field : record.fields()) {
            writer.writeFieldName(field);
            writer.writeObject(record.getFieldValue(field));
          }
          writer.writeEndObject();
        } catch (IOException e) {
          throw new UncheckedIOException(e.getMessage(), e);
        }
        currentLine++;
      }

      @Override
      protected void hookOnError(Throwable t) {
        LOGGER.error(String.format("Error writing to %s: %s", url, t.getMessage()), t);
      }

      @Override
      protected void hookOnComplete() {
        LOGGER.debug("Done writing {}", url);
      }

      @Override
      protected void hookFinally(SignalType type) {
        end(type);
      }

      private void end(SignalType type) {
        if (writer != null) {
          try {
            if (type == SignalType.ON_COMPLETE) {
              // add one last EOL if the file completed successfully; the writer doesn't do it by default
              writer.writeRaw(lineSeparator);
            }
            writer.close();
          } catch (IOException e) {
            LOGGER.error(String.format("Could not close %s: %s", url, e.getMessage()), e);
          }
        }
      }
    };
  }

  private Subscriber<Record> multipleWriteSubscriber() {
    threadPool =
        Executors.newFixedThreadPool(
            maxConcurrentFiles,
            new ThreadFactoryBuilder().setNameFormat("json-connector-%d").build());
    writeQueueProcessor = WorkQueueProcessor.<Record>builder().executor(threadPool).build();
    for (int i = 0; i < maxConcurrentFiles; i++) {
      writeQueueProcessor.subscribe(singleWriteSubscriber());
    }
    return writeQueueProcessor;
  }

  private JsonGenerator createJsonWriter(URL url) {
    try {
      JsonFactory factory = objectMapper.getFactory();
      JsonGenerator writer = factory.createGenerator(IOUtils.newBufferedWriter(url, encoding));
      writer.setRootValueSeparator(new SerializedString(lineSeparator));
      return writer;
    } catch (Exception e) {
      LOGGER.error(
          String.format("Could not create Json writer for %s: %s", url, e.getMessage()), e);
      throw new RuntimeException(e);
    }
  }

  private URL getOrCreateDestinationURL() {
    if (root != null) {
      try {
        String next = String.format(fileNameFormat, counter.incrementAndGet());
        return root.resolve(next).toUri().toURL();
      } catch (Exception e) {
        LOGGER.error(
            String.format(
                "Could not create file URL with format %s: %s", fileNameFormat, e.getMessage()),
            e);
        throw new RuntimeException(e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return url;
  }
}
