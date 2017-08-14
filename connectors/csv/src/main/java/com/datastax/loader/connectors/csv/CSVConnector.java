/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.csv;

import static java.nio.file.FileVisitResult.CONTINUE;
import static java.nio.file.FileVisitResult.TERMINATE;

import com.datastax.loader.connectors.api.Connector;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.connectors.api.internal.MapRecord;
import com.typesafe.config.Config;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.csv.CsvFormat;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collections;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A connector for CSV files.
 *
 * <p>It is capable of reading from any URL, provided that there is a {@link
 * java.net.URLStreamHandler handler} installed for it. For file URLs, it is also capable of reading
 * several files at once from a given root directory.
 *
 * <p>This connector is highly configurable; see its {@code reference.conf} file, bundled within its
 * jar archive, for detailed information.
 */
public class CSVConnector implements Connector {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVConnector.class);

  private URL url;
  private Path root;
  private String pattern;
  private Charset encoding;
  private char delimiter;
  private char quote;
  private char escape;
  private char comment;
  private long linesToSkip;
  private long maxLines;
  private int maxThreads;
  private boolean recursive;
  private boolean header;
  private CsvParserSettings settings;

  @Override
  public Config configure(Config settings) throws MalformedURLException {
    // Create a Config object that effectively merges the csv level with
    // the connector level. The csv level in settings is a merge of
    // user-provided values and the values from reference.conf. That is
    // our fallback (e.g. think attributes like header, url under csv, but
    // where the root is now csv).
    // If the user specifies overrides like config.comment, those
    // take precedence over those specified in csv.
    // The net effect is that a user can specify a command-line override
    // like connector.csv.comment, and it'll be respected, while
    // at the same time an override like connector.url will also be
    // respected.

    settings = settings.withoutPath("csv").withFallback(settings.getConfig("csv"));
    url = parseUrlOrPath(settings.getString("url"));
    pattern = settings.getString("pattern");
    encoding = Charset.forName(settings.getString("encoding"));
    delimiter = getAsChar(settings, "delimiter");
    quote = getAsChar(settings, "quote");
    escape = getAsChar(settings, "escape");
    comment = getAsChar(settings, "comment");
    linesToSkip = settings.getLong("linesToSkip");
    maxLines = settings.getLong("maxLines");
    maxThreads = settings.getInt("maxThreads");
    recursive = settings.getBoolean("recursive");
    header = settings.getBoolean("header");
    return settings;
  }

  @Override
  public void init() throws URISyntaxException, IOException {
    try {
      Path root = Paths.get(url.toURI());
      if (Files.isDirectory(root)) {
        this.root = root;
      }
    } catch (FileSystemNotFoundException ignored) {
      // not a path on a known filesystem
    }
    CsvFormat format = new CsvFormat();
    format.setDelimiter(delimiter);
    format.setQuote(quote);
    format.setQuoteEscape(escape);
    format.setComment(comment);
    settings = new CsvParserSettings();
    settings.setFormat(format);
    settings.setNumberOfRowsToSkip(linesToSkip);
    settings.setHeaderExtractionEnabled(header);
  }

  @Override
  public Publisher<Record> read() {
    if (root != null) {
      return Flowable.merge(
          scan(root).map(p -> records(p.toUri().toURL()).subscribeOn(Schedulers.io())), maxThreads);
    } else {
      return records(url);
    }
  }

  @NotNull
  private static URL parseUrlOrPath(String urlOrPath) {
    // NOTE: This is a good candidate for moving into a commons module / utility class.
    // This logic also exists in SettingsUtils, but can't be directly referenced
    // here due to circular dependencies between modules.

    try {
      return new URL(urlOrPath);
    } catch (MalformedURLException e) {
      // Parsing failed, so guess that it's a file path and prepend it
      // to make a valid url.
      try {
        return new URL("file:" + urlOrPath);
      } catch (MalformedURLException e1) {
        // Still bad...
        throw new IllegalArgumentException("Invalid URL: " + urlOrPath, e1);
      }
    }
  }

  private Flowable<Path> scan(Path root) {
    return Flowable.create(
        e -> {
          PathMatcher matcher = root.getFileSystem().getPathMatcher("glob:" + pattern);
          Files.walkFileTree(
              root,
              Collections.emptySet(),
              recursive ? Integer.MAX_VALUE : 1,
              new SimpleFileVisitor<Path>() {

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                    throws IOException {
                  return e.isCancelled() ? TERMINATE : CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs)
                    throws IOException {
                  if (Files.isReadable(file)
                      && Files.isRegularFile(file)
                      && matcher.matches(file)
                      && !e.isCancelled()) {
                    LOGGER.debug("Emitting file {}", file);
                    e.onNext(file);
                  }
                  return e.isCancelled() ? TERMINATE : CONTINUE;
                }

                @Override
                public FileVisitResult visitFileFailed(Path file, IOException ex)
                    throws IOException {
                  LOGGER.warn("Could not read " + file.toAbsolutePath().toUri().toURL(), e);
                  return e.isCancelled() ? TERMINATE : CONTINUE;
                }
              });
          e.onComplete();
        },
        BackpressureStrategy.BUFFER);
  }

  private Flowable<Record> records(URL url) {
    Flowable<Record> records =
        Flowable.create(
            e -> {
              CsvParser parser = new CsvParser(settings);
              try (InputStream is = openStream(url)) {
                parser.beginParsing(is, encoding);
                while (true) {
                  com.univocity.parsers.common.record.Record row = parser.parseNextRecord();
                  ParsingContext context = parser.getContext();
                  String source = context.currentParsedContent();
                  URL location = getCurrentLocation(url, context);
                  if (row == null) {
                    break;
                  }
                  if (e.isCancelled()) {
                    break;
                  }
                  Record record;
                  if (header) {
                    record =
                        new MapRecord(source, location, context.parsedHeaders(), row.getValues());
                  } else {
                    record = new MapRecord(source, location, (Object[]) row.getValues());
                  }
                  LOGGER.trace("Emitting record {}", record);
                  e.onNext(record);
                }
                e.onComplete();
                parser.stopParsing();
              }
            },
            BackpressureStrategy.BUFFER);
    if (maxLines != -1) {
      records = records.take(maxLines);
    }
    return records;
  }

  private URL getCurrentLocation(URL url, ParsingContext context) {
    URL location;
    try {
      long line = context.currentLine();
      int column = context.currentColumn();
      location =
          new URL(
              url.toExternalForm()
                  + (url.getQuery() == null ? '?' : '&')
                  + "?line="
                  + line
                  + "&column="
                  + column);
    } catch (MalformedURLException e) {
      // should not happen, but no reason to fail for that
      location = url;
    }
    return location;
  }

  private static char getAsChar(Config settings, String path) {
    String setting = settings.getString(path);
    if (setting.length() != 1) {
      throw new IllegalArgumentException(
          String.format(
              "Invalid setting for %s: expecting a single character, got '%s'", path, setting));
    }
    return setting.charAt(0);
  }

  private static InputStream openStream(URL url) throws IOException {
    InputStream in = url.openStream();
    return in instanceof BufferedInputStream ? in : new BufferedInputStream(in);
  }
}
