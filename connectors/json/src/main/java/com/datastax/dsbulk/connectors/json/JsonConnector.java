/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.json;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.io.CompressedIOUtils;
import com.datastax.dsbulk.commons.internal.reactive.SimpleBackpressureController;
import com.datastax.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.dsbulk.connectors.api.MappedField;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.connectors.api.RecordMetadata;
import com.datastax.dsbulk.connectors.api.internal.DefaultMappedField;
import com.datastax.dsbulk.connectors.api.internal.DefaultRecord;
import com.datastax.dsbulk.connectors.commons.AbstractFileBasedConnector;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
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
import com.typesafe.config.ConfigException;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLStreamHandler;
import java.nio.channels.ClosedChannelException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;

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
public class JsonConnector extends AbstractFileBasedConnector {

  enum DocumentMode {
    MULTI_DOCUMENT,
    SINGLE_DOCUMENT
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(JsonConnector.class);

  private static final GenericType<JsonNode> JSON_NODE_TYPE_TOKEN = GenericType.of(JsonNode.class);

  private static final String MODE = "mode";
  private static final String PARSER_FEATURES = "parserFeatures";
  private static final String GENERATOR_FEATURES = "generatorFeatures";
  private static final String SERIALIZATION_FEATURES = "serializationFeatures";
  private static final String DESERIALIZATION_FEATURES = "deserializationFeatures";
  private static final String SERIALIZATION_STRATEGY = "serializationStrategy";
  private static final String PRETTY_PRINT = "prettyPrint";

  private static final TypeReference<Map<String, JsonNode>> JSON_NODE_MAP_TYPE_REFERENCE =
      new TypeReference<Map<String, JsonNode>>() {};

  private DocumentMode mode;
  private ObjectMapper objectMapper;
  private JavaType jsonNodeMapType;
  private Map<JsonParser.Feature, Boolean> parserFeatures;
  private Map<JsonGenerator.Feature, Boolean> generatorFeatures;
  private Map<SerializationFeature, Boolean> serializationFeatures;
  private Map<DeserializationFeature, Boolean> deserializationFeatures;
  private JsonInclude.Include serializationStrategy;
  private boolean prettyPrint;

  @NonNull
  public String getConnectorName() {
    return "json";
  }

  @Override
  public void configure(@NonNull LoaderConfig settings, boolean read) {
    try {
      super.configure(settings, read);
      mode = settings.getEnum(DocumentMode.class, MODE);
      parserFeatures = getFeatureMap(settings.getConfig(PARSER_FEATURES), JsonParser.Feature.class);
      generatorFeatures =
          getFeatureMap(settings.getConfig(GENERATOR_FEATURES), JsonGenerator.Feature.class);
      serializationFeatures =
          getFeatureMap(settings.getConfig(SERIALIZATION_FEATURES), SerializationFeature.class);
      deserializationFeatures =
          getFeatureMap(settings.getConfig(DESERIALIZATION_FEATURES), DeserializationFeature.class);
      serializationStrategy = settings.getEnum(JsonInclude.Include.class, SERIALIZATION_STRATEGY);
      prettyPrint = settings.getBoolean(PRETTY_PRINT);
    } catch (ConfigException e) {
      throw BulkConfigurationException.fromTypeSafeConfigException(e, "dsbulk.connector.json");
    }
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    super.init();
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
      if (prettyPrint) {
        objectMapper.setDefaultPrettyPrinter(new DefaultPrettyPrinter(System.lineSeparator()));
      }
      objectMapper.setSerializationInclusion(serializationStrategy);
    }
  }

  @NonNull
  @Override
  public RecordMetadata getRecordMetadata() {
    return (field, cqlType) -> JSON_NODE_TYPE_TOKEN;
  }

  @Override
  public boolean supports(@NonNull ConnectorFeature feature) {
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

  @NonNull
  public Flux<Record> readSingleFile(@NonNull URL url) {
    return Flux.create(
        sink -> {
          LOGGER.debug("Reading {}", url);
          URI resource = URI.create(url.toExternalForm());
          SimpleBackpressureController controller = new SimpleBackpressureController();
          sink.onRequest(controller::signalRequested);
          // DAT-177: Do not call sink.onDispose nor sink.onCancel,
          // as doing so seems to prevent the flow from completing in rare occasions.
          JsonFactory factory = objectMapper.getFactory();
          try (BufferedReader r = CompressedIOUtils.newBufferedReader(url, encoding, compression);
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
  }

  @NonNull
  @Override
  protected RecordWriter newSingleFileWriter() {
    return new JsonWriter();
  }

  private class JsonWriter implements RecordWriter {

    private URL url;
    private JsonGenerator writer;
    private long currentLine;

    @Override
    public void write(@NonNull Record record) throws IOException {
      try {
        if (writer == null) {
          open();
        } else if (shouldRoll()) {
          close();
          open();
        }
        LOGGER.trace("Writing record {}", record);
        if (mode == DocumentMode.SINGLE_DOCUMENT && currentLine > 0) {
          writer.writeRaw(',');
        }

        writer.writeObject(record);
        currentLine++;
      } catch (ClosedChannelException e) {
        // OK, happens when the channel was closed due to interruption
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
        writer = newJsonGenerator(url);
        if (mode == DocumentMode.SINGLE_DOCUMENT) {
          // do not use writer.writeStartArray(): we need to fool the parser into thinking it's on
          // multi doc mode,
          // to get a better-looking result
          writer.writeRaw('[');
          writer.writeRaw(System.lineSeparator());
        }
        currentLine = 0;
        LOGGER.debug("Writing " + url);
      } catch (ClosedChannelException e) {
        // OK, happens when the channel was closed due to interruption
      } catch (RuntimeException | IOException e) {
        throw new IOException(String.format("Error opening %s", url), e);
      }
    }

    @Override
    public void close() throws IOException {
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
        } catch (ClosedChannelException e) {
          // OK, happens when the channel was closed due to interruption
        } catch (RuntimeException | IOException e) {
          throw new IOException(String.format("Error closing %s", url), e);
        }
      }
    }
  }

  private JsonGenerator newJsonGenerator(URL url) throws IOException {
    JsonFactory factory = objectMapper.getFactory();
    JsonGenerator generator =
        factory.createGenerator(CompressedIOUtils.newBufferedWriter(url, encoding, compression));
    generator.setRootValueSeparator(new SerializedString(System.lineSeparator()));
    return generator;
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
