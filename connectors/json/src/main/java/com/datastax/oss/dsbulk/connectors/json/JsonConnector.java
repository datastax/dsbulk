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
package com.datastax.oss.dsbulk.connectors.json;

import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.datastax.oss.dsbulk.connectors.api.CommonConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.ConnectorFeature;
import com.datastax.oss.dsbulk.connectors.api.DefaultMappedField;
import com.datastax.oss.dsbulk.connectors.api.DefaultRecord;
import com.datastax.oss.dsbulk.connectors.api.MappedField;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.connectors.api.RecordMetadata;
import com.datastax.oss.dsbulk.connectors.commons.AbstractFileBasedConnector;
import com.datastax.oss.dsbulk.io.CompressedIOUtils;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.io.SerializedString;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.typesafe.config.Config;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.SynchronousSink;

/**
 * A connector for Json files.
 *
 * <p>It is capable of reading from any URL, provided that there is a {@link URLStreamHandler
 * handler} installed for it. For file URLs, it is also capable of reading several files at once
 * from a given root directory.
 *
 * <p>This connector is highly configurable; see its {@code dsbulk-reference.conf} file, bundled
 * within its jar archive, for detailed information.
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

  private DocumentMode mode;
  private ObjectMapper objectMapper;
  private Map<JsonParser.Feature, Boolean> parserFeatures;
  private Map<JsonGenerator.Feature, Boolean> generatorFeatures;
  private Map<SerializationFeature, Boolean> serializationFeatures;
  private Map<DeserializationFeature, Boolean> deserializationFeatures;
  private JsonInclude.Include serializationStrategy;
  private boolean prettyPrint;

  @Override
  @NonNull
  public String getConnectorName() {
    return "json";
  }

  @Override
  public void configure(@NonNull Config settings, boolean read, boolean retainRecordSources) {
    try {
      super.configure(settings, read, retainRecordSources);
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
      throw ConfigUtils.convertConfigException(e, "dsbulk.connector.json");
    }
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    super.init();
    objectMapper = new ObjectMapper();
    objectMapper.setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
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
        case DATA_SIZE_SAMPLING:
          return isDataSizeSamplingAvailable();
      }
    }
    return false;
  }

  @Override
  @NonNull
  protected RecordReader newSingleFileReader(@NonNull URL url) throws IOException {
    return new JsonRecordReader(url);
  }

  private class JsonRecordReader implements RecordReader {

    private final URL url;
    private final URI resource;
    private final JsonParser parser;
    private final MappingIterator<JsonNode> nodesIterator;

    private long recordNumber = 1;

    private JsonRecordReader(URL url) throws IOException {
      this.url = url;
      resource = URI.create(url.toExternalForm());
      try {
        JsonFactory factory = objectMapper.getFactory();
        BufferedReader r = CompressedIOUtils.newBufferedReader(url, encoding, compression);
        parser = factory.createParser(r);
        if (mode == DocumentMode.SINGLE_DOCUMENT) {
          do {
            parser.nextToken();
          } while (parser.currentToken() != JsonToken.START_ARRAY && parser.currentToken() != null);
          parser.nextToken();
        }
        nodesIterator = objectMapper.readValues(parser, JsonNode.class);
      } catch (Exception e) {
        throw new IOException(String.format("Error reading from %s", url), e);
      }
    }

    @NonNull
    @Override
    public RecordReader readNext(@NonNull SynchronousSink<Record> sink) {
      try {
        if (nodesIterator.hasNext()) {
          if (parser.currentToken() != JsonToken.START_OBJECT) {
            throw new JsonParseException(
                parser,
                String.format(
                    "Expecting START_OBJECT, got %s. Did you forget to set connector.json.mode to SINGLE_DOCUMENT?",
                    parser.currentToken()));
          }
          JsonNode source = nodesIterator.next();
          Map<MappedField, JsonNode> fields = new HashMap<>();
          Iterator<Entry<String, JsonNode>> children = source.fields();
          while (children.hasNext()) {
            Entry<String, JsonNode> child = children.next();
            fields.put(new DefaultMappedField(child.getKey()), child.getValue());
          }
          Record record =
              DefaultRecord.mapped(
                  retainRecordSources ? source : null, resource, recordNumber++, fields);
          LOGGER.trace("Emitting record {}", record);
          sink.next(record);
        } else {
          LOGGER.debug("Done reading {}", url);
          sink.complete();
        }
      } catch (Exception e) {
        sink.error(new IOException(String.format("Error reading from %s", url), e));
      }
      return this;
    }

    @Override
    public void close() throws IOException {
      if (parser != null) {
        parser.close();
      }
    }
  }

  @NonNull
  @Override
  protected RecordWriter newSingleFileWriter() {
    return new JsonRecordWriter();
  }

  private class JsonRecordWriter implements RecordWriter {

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
    public void flush() throws IOException {
      if (writer != null) {
        writer.flush();
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
      Config source, Class<T> featureClass) {
    Map<T, Boolean> dest = new HashMap<>();
    for (String name : source.root().keySet()) {
      dest.put(Enum.valueOf(featureClass, name), source.getBoolean(name));
    }
    return dest;
  }
}
