/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.json;

import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.getURLsFromFile;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isPathAbsentOrEmpty;
import static com.datastax.dsbulk.commons.internal.config.ConfigUtils.isPathPresentAndNotEmpty;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.ConfigUtils;
import com.datastax.dsbulk.commons.internal.io.IOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.dsbulk.connectors.api.Connector;
import com.datastax.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.dsbulk.connectors.api.MappedField;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.commons.internal.CompressedIOUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.ConfigException;
import io.netty.util.concurrent.DefaultThreadFactory;
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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Signal;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

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
  private static final TypeToken<JsonNode> JSON_NODE_TYPE_TOKEN = TypeToken.of(JsonNode.class);

  private static final String URL = "url";
  private static final String URLFILE = "urlfile";
  private static final String MODE = "mode";
  private static final String FILE_NAME_PATTERN = "fileNamePattern";
  private static final String ENCODING = "encoding";
  private static final String COMPRESSION = "compression";
  private static final String SKIP_RECORDS = "skipRecords";
  private static final String MAX_RECORDS = "maxRecords";
  private static final String MAX_CONCURRENT_FILES = "maxConcurrentFiles";
  private static final String RECURSIVE = "recursive";
  private static final String FILE_NAME_FORMAT = "fileNameFormat";
  private static final String PARSER_FEATURES = "parserFeatures";
  private static final String GENERATOR_FEATURES = "generatorFeatures";
  private static final String SERIALIZATION_FEATURES = "serializationFeatures";
  private static final String DESERIALIZATION_FEATURES = "deserializationFeatures";
  private static final String SERIALIZATION_STRATEGY = "serializationStrategy";
  private static final String PRETTY_PRINT = "prettyPrint";
  private static final String FLUSH_WINDOW = "flushWindow";

  private static final TypeReference<Map<String, JsonNode>> JSON_NODE_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, JsonNode>>() {};

  private boolean read;
  private List<URL> urls;
  private DocumentMode mode;
  private List<Path> roots;
  private List<URL> files;
  private String pattern;
  private Charset encoding;
  private String compression;
  private long skipRecords;
  private long maxRecords;
  private int maxConcurrentFiles;
  private boolean recursive;
  private String fileNameFormat;
  private int resourceCount;
  @VisibleForTesting AtomicInteger counter;
  private ObjectMapper objectMapper;
  private JavaType jsonNodeMapType;
  private Map<JsonParser.Feature, Boolean> parserFeatures;
  private Map<JsonGenerator.Feature, Boolean> generatorFeatures;
  private Map<SerializationFeature, Boolean> serializationFeatures;
  private Map<DeserializationFeature, Boolean> deserializationFeatures;
  private JsonInclude.Include serializationStrategy;
  private boolean prettyPrint;
  private int flushWindow;
  private Scheduler scheduler;
  private List<JsonWriter> writers;

  @Override
  public void configure(LoaderConfig settings, boolean read) {
    try {
      validateURL(settings, read);
      this.read = read;
      urls = loadURLs(settings);
      roots = new ArrayList<>();
      files = new ArrayList<>();
      mode = settings.getEnum(DocumentMode.class, MODE);
      pattern = settings.getString(FILE_NAME_PATTERN);
      encoding = settings.getCharset(ENCODING);
      compression = settings.getString(COMPRESSION);
      if (!CompressedIOUtils.isSupportedCompression(compression, read)) {
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for connector.json.%s, valid values: %s, got: '%s'",
                COMPRESSION,
                String.join(",", CompressedIOUtils.getSupportedCompressions(read)),
                compression));
      }
      skipRecords = settings.getLong(SKIP_RECORDS);
      maxRecords = settings.getLong(MAX_RECORDS);
      maxConcurrentFiles = settings.getThreads(MAX_CONCURRENT_FILES);
      recursive = settings.getBoolean(RECURSIVE);
      fileNameFormat = settings.getString(FILE_NAME_FORMAT);
      if (!CompressedIOUtils.isNoneCompression(compression)
          && fileNameFormat.toLowerCase().endsWith(".json")) {
        fileNameFormat = fileNameFormat + CompressedIOUtils.getCompressionSuffix(compression);
      }
      parserFeatures = getFeatureMap(settings.getConfig(PARSER_FEATURES), JsonParser.Feature.class);
      generatorFeatures =
          getFeatureMap(settings.getConfig(GENERATOR_FEATURES), JsonGenerator.Feature.class);
      serializationFeatures =
          getFeatureMap(settings.getConfig(SERIALIZATION_FEATURES), SerializationFeature.class);
      deserializationFeatures =
          getFeatureMap(settings.getConfig(DESERIALIZATION_FEATURES), DeserializationFeature.class);
      serializationStrategy = settings.getEnum(JsonInclude.Include.class, SERIALIZATION_STRATEGY);
      prettyPrint = settings.getBoolean(PRETTY_PRINT);
      flushWindow = settings.getInt(FLUSH_WINDOW);
      if (flushWindow < 1) {
        throw new BulkConfigurationException(
            String.format(
                "Invalid value for connector.json.%s: Expecting integer > 0, got: %d",
                FLUSH_WINDOW, flushWindow));
      }
    } catch (ConfigException e) {
      throw ConfigUtils.configExceptionToBulkConfigurationException(e, "connector.json");
    }
  }

  @NotNull
  private List<URL> loadURLs(LoaderConfig settings) {
    if (isPathPresentAndNotEmpty(settings, URLFILE)) {
      // suppress URL option
      try {
        return getURLsFromFile(settings.getPath(URLFILE));
      } catch (IOException e) {
        throw new BulkConfigurationException(
            "Problem when retrieving urls from file specified by the URL file parameter", e);
      }
    } else {
      return Collections.singletonList(settings.getURL(URL));
    }
  }

  private void validateURL(LoaderConfig settings, boolean read) {
    if (read) {
      // for LOAD
      if (isPathAbsentOrEmpty(settings, URL)) {
        if (isPathAbsentOrEmpty(settings, URLFILE)) {
          throw new BulkConfigurationException(
              "A URL or URL file is mandatory when using the json connector for LOAD. Please set connector.json.url or connector.json.urlfile "
                  + "and try again. See settings.md or help for more information.");
        }
      }
      if (isPathPresentAndNotEmpty(settings, URL) && isPathPresentAndNotEmpty(settings, URLFILE)) {
        LOGGER.debug("You specified both URL and URL file. The URL file will take precedence.");
      }
    } else {
      // for UNLOAD we are not supporting urlfile parameter
      if (isPathPresentAndNotEmpty(settings, URLFILE)) {
        throw new BulkConfigurationException("The urlfile parameter is not supported for UNLOAD");
      }
      if (isPathAbsentOrEmpty(settings, URL)) {
        throw new BulkConfigurationException(
            "A URL is mandatory when using the json connector for UNLOAD. Please set connector.json.url "
                + "and try again. See settings.md or help for more information.");
      }
    }
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    if (read) {
      tryReadFromDirectories();
    } else {
      tryWriteToDirectory();
    }
    objectMapper = new ObjectMapper();
    objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    jsonNodeMapType = objectMapper.constructType(JSON_NODE_MAP_TYPE_REFERENCE.getType());
    if (read) {
      for (JsonParser.Feature parserFeature : parserFeatures.keySet()) {
        objectMapper.configure(parserFeature, parserFeatures.get(parserFeature));
      }
      for (DeserializationFeature deserializationFeature : deserializationFeatures.keySet()) {
        objectMapper.configure(
            deserializationFeature, deserializationFeatures.get(deserializationFeature));
      }
    } else {
      for (JsonGenerator.Feature generatorFeature : generatorFeatures.keySet()) {
        objectMapper.configure(generatorFeature, generatorFeatures.get(generatorFeature));
      }
      for (SerializationFeature serializationFeature : serializationFeatures.keySet()) {
        objectMapper.configure(
            serializationFeature, serializationFeatures.get(serializationFeature));
      }
      counter = new AtomicInteger(0);
      if (prettyPrint) {
        objectMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter(System.lineSeparator()));
      }
      objectMapper.setSerializationInclusion(serializationStrategy);
      writers = new CopyOnWriteArrayList<>();
      if (writeConcurrency() > 1) {
        ThreadFactory threadFactory = new DefaultThreadFactory("csv-connector");
        scheduler =
            maxConcurrentFiles == 1
                ? Schedulers.newSingle(threadFactory)
                : Schedulers.newParallel(maxConcurrentFiles, threadFactory);
      }
      for (int i = 0; i < maxConcurrentFiles; i++) {
        writers.add(new JsonWriter());
      }
    }
  }

  @Override
  public RecordMetadata getRecordMetadata() {
    return (field, cqlType) -> JSON_NODE_TYPE_TOKEN;
  }

  @Override
  public boolean supports(ConnectorFeature feature) {
    if (feature instanceof CommonConnectorFeature) {
      CommonConnectorFeature commonFeature = (CommonConnectorFeature) feature;
      switch (commonFeature) {
        case MAPPED_RECORDS:
          return true;
        case INDEXED_RECORDS:
          return false;
      }
    }
    return false;
  }

  @Override
  public void close() {
    if (scheduler != null) {
      scheduler.dispose();
    }
    if (writers != null) {
      IOException e = null;
      for (JsonWriter writer : writers) {
        try {
          writer.close();
        } catch (IOException e1) {
          if (e == null) {
            e = e1;
          } else {
            e.addSuppressed(e1);
          }
        }
      }
      if (e != null) {
        throw new UncheckedIOException(e);
      }
    }
  }

  @Override
  public int estimatedResourceCount() {
    return resourceCount;
  }

  @Override
  public Publisher<Record> read() {
    assert read;
    return Flux.concat(
        Flux.fromIterable(roots).flatMap(this::scanRootDirectory).flatMap(this::readURL),
        Flux.fromIterable(files).flatMap(this::readURL));
  }

  @Override
  public Publisher<Publisher<Record>> readByResource() {
    assert read;
    return Flux.concat(
        Flux.fromIterable(roots).flatMap(this::scanRootDirectory).map(this::readURL),
        Flux.fromIterable(files).map(this::readURL));
  }

  @Override
  public Function<? super Publisher<Record>, ? extends Publisher<Record>> write() {
    assert !read;
    if (writeConcurrency() > 1) {
      return upstream ->
          Flux.from(upstream)
              .parallel(maxConcurrentFiles)
              .runOn(scheduler)
              .groups()
              .flatMap(rail -> rail.transform(writeRecords(Objects.requireNonNull(rail.key()))));
    } else {
      return upstream -> Flux.from(upstream).transform(writeRecords(0));
    }
  }

  private Function<Flux<Record>, Flux<Record>> writeRecords(int key) {
    JsonWriter writer = writers.get(key);
    return records ->
        records
            .window(flushWindow)
            .flatMap(
                window -> {
                  return window
                      .materialize()
                      .map(
                          signal -> {
                            if (signal.isOnNext()) {
                              Record record = signal.get();
                              assert record != null;
                              try {
                                writer.write(record);
                              } catch (Exception e) {
                                signal = Signal.error(e);
                              }
                            }
                            return signal;
                          })
                      .dematerialize();
                });
  }

  private int writeConcurrency() {
    // when unloading, roots can only be empty or contain one element; if it's empty, we are writing
    // to a single file and the write concurrency is necessarily 1; if it is not empty, we are
    // writing to a directory and the write concurrency is maxConcurrentFiles.
    return roots.isEmpty() ? 1 : maxConcurrentFiles;
  }

  private void tryReadFromDirectories() throws URISyntaxException, IOException {
    resourceCount = 0;
    for (URL u : urls) {
      try {
        Path root = Paths.get(u.toURI());
        if (Files.isDirectory(root)) {
          if (!Files.isReadable(root)) {
            throw new IllegalArgumentException(
                String.format("Directory is not readable: %s.", root));
          }
          roots.add(root);
          int inDirectoryResourceCount =
              Objects.requireNonNull(scanRootDirectory(root).take(100).count().block()).intValue();
          if (inDirectoryResourceCount == 0) {
            if (IOUtils.countReadableFiles(root, recursive) == 0) {
              LOGGER.warn("Directory {} has no readable files.", root);
            } else {
              String formatString =
                  "No files in directory {} matched the connector.json.fileNamePattern of \"{}\".";
              if (!CompressedIOUtils.isNoneCompression(compression)) {
                formatString =
                    formatString + " Adjust it if connector.json.compression is specified!";
              }
              LOGGER.warn(formatString, root, pattern);
            }
          }
          resourceCount += inDirectoryResourceCount;
        } else {
          resourceCount += 1;
          files.add(u);
        }
      } catch (FileSystemNotFoundException ignored) {
        files.add(u);
        // not a path on a known filesystem, fall back to reading from URL directly
      }
    }
  }

  private void tryWriteToDirectory() throws URISyntaxException, IOException {
    try {
      resourceCount = -1;
      Path root = Paths.get(urls.get(0).toURI()); // for UNLOAD always one URL
      if (!Files.exists(root)) {
        root = Files.createDirectories(root);
      }
      if (Files.isDirectory(root)) {
        if (!Files.isWritable(root)) {
          throw new IllegalArgumentException(String.format("Directory is not writable: %s.", root));
        }
        if (IOUtils.isDirectoryNonEmpty(root)) {
          throw new IllegalArgumentException(
              "Invalid value for connector.json.url: target directory " + root + " must be empty.");
        }
        this.roots.add(root);
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
              URI resource = URI.create(url.toExternalForm());
              SimpleBackpressureController controller = new SimpleBackpressureController();
              sink.onRequest(controller::signalRequested);
              // DAT-177: Do not call sink.onDispose nor sink.onCancel,
              // as doing so seems to prevent the flow from completing in rare occasions.
              JsonFactory factory = objectMapper.getFactory();
              try (BufferedReader r =
                      CompressedIOUtils.newBufferedReader(url, encoding, compression);
                  JsonParser parser = factory.createParser(r)) {
                if (mode == DocumentMode.SINGLE_DOCUMENT) {
                  do {
                    parser.nextToken();
                  } while (parser.currentToken() != JsonToken.START_ARRAY
                      && parser.currentToken() != null);
                  parser.nextToken();
                }
                MappingIterator<JsonNode> it = objectMapper.readValues(parser, JsonNode.class);
                long recordNumber = 1;
                while (!sink.isCancelled() && it.hasNext()) {
                  if (parser.currentToken() != JsonToken.START_OBJECT) {
                    throw new JsonParseException(
                        parser,
                        String.format(
                            "Expecting START_OBJECT, got %s. Did you forget to set connector.json.mode to SINGLE_DOCUMENT?",
                            parser.currentToken()));
                  }
                  Record record;
                  JsonNode node = it.next();
                  Map<String, JsonNode> values = objectMapper.convertValue(node, jsonNodeMapType);
                  Map<MappedField, JsonNode> fields =
                      values.entrySet().stream()
                          .collect(
                              Collectors.toMap(
                                  e -> new DefaultMappedField(e.getKey()), Entry::getValue));
                  record = DefaultRecord.mapped(node, resource, recordNumber++, fields);
                  LOGGER.trace("Emitting record {}", record);
                  controller.awaitRequested(1);
                  sink.next(record);
                }
                LOGGER.debug("Done reading {}", url);
                sink.complete();
              } catch (Exception e) {
                sink.error(new IOException(String.format("Error reading from %s", url), e));
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

  private Flux<URL> scanRootDirectory(Path root) {
    try {
      // this stream will be closed by the flux, do not add it to a try-with-resources block
      @SuppressWarnings("StreamResourceLeak")
      Stream<Path> files = Files.walk(root, recursive ? Integer.MAX_VALUE : 1);
      PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
      return Flux.fromStream(files)
          .filter(Files::isReadable)
          .filter(Files::isRegularFile)
          .filter(matcher::matches)
          .map(
              file -> {
                try {
                  return file.toUri().toURL();
                } catch (MalformedURLException e) {
                  throw new UncheckedIOException(e);
                }
              });
    } catch (IOException e) {
      throw new UncheckedIOException("Error scanning directory " + root, e);
    }
  }

  private class JsonWriter {

    private URL url;
    private JsonGenerator writer;
    private long currentLine;

    private void write(Record record) throws IOException {
      try {
        if (writer == null) {
          open();
        } else if (shouldRoll()) {
          close();
          open();
        }
        LOGGER.trace("Writing record {} to {}", record, url);
        if (mode == DocumentMode.SINGLE_DOCUMENT && currentLine > 0) {
          writer.writeRaw(',');
        }
        writer.writeObject(record);
        currentLine++;
      } catch (RuntimeException e) {
        throw new IOException(String.format("Error writing to %s", url), e);
      }
    }

    private boolean shouldRoll() {
      return !roots.isEmpty() && currentLine == maxRecords;
    }

    private void open() throws IOException {
      url = getOrCreateDestinationURL();
      try {
        writer = createJsonWriter(url);
        if (mode == DocumentMode.SINGLE_DOCUMENT) {
          // do not use writer.writeStartArray(): we need to fool the parser into thinking it's on
          // multi doc mode, to get a better-looking result
          writer.writeRaw('[');
          writer.writeRaw(System.lineSeparator());
        }
        currentLine = 0;
        LOGGER.debug("Writing " + url);
      } catch (RuntimeException | IOException e) {
        throw new IOException(String.format("Error opening %s", url), e);
      }
    }

    private void close() throws IOException {
      if (writer != null) {
        try {
          // add one last EOL before closing; the writer doesn't do it by default
          writer.writeRaw(System.lineSeparator());
          if (mode == DocumentMode.SINGLE_DOCUMENT) {
            writer.writeRaw(']');
            writer.writeRaw(System.lineSeparator());
          }
          writer.close();
          LOGGER.debug("Done writing {}", url);
          writer = null;
        } catch (RuntimeException | IOException e) {
          throw new IOException(String.format("Error closing %s", url), e);
        }
      }
    }
  }

  private JsonGenerator createJsonWriter(URL url) throws IOException {
    JsonFactory factory = objectMapper.getFactory();
    JsonGenerator writer =
        factory.createGenerator(CompressedIOUtils.newBufferedWriter(url, encoding, compression));
    writer.setRootValueSeparator(new SerializedString(System.lineSeparator()));
    return writer;
  }

  @VisibleForTesting
  URL getOrCreateDestinationURL() {
    if (!roots.isEmpty()) {
      try {
        String next = String.format(fileNameFormat, counter.incrementAndGet());
        return roots.get(0).resolve(next).toUri().toURL(); // for UNLOAD always one URL
      } catch (MalformedURLException e) {
        throw new UncheckedIOException(
            String.format("Could not create file URL with format %s", fileNameFormat), e);
      }
    }
    // assume we are writing to a single URL and ignore fileNameFormat
    return urls.get(0); // for UNLOAD always one URL
  }

  private static <T extends Enum<T>> Map<T, Boolean> getFeatureMap(
      LoaderConfig source, Class<T> featureClass) {
    Map<T, Boolean> dest = new HashMap<>();
    for (String name : source.root().keySet()) {
      dest.put(Enum.valueOf(featureClass, name), source.getBoolean(name));
    }
    return dest;
  }
}
