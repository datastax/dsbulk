/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log;

import static com.datastax.dsbulk.engine.internal.log.LogUtils.appendRecordInfo;
import static com.datastax.dsbulk.engine.internal.log.LogUtils.appendStatementInfo;
import static com.datastax.dsbulk.engine.internal.log.LogUtils.printAndMaybeAddNewLine;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.commons.internal.uri.URIUtils;
import com.datastax.dsbulk.connectors.api.Record;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.dsbulk.engine.internal.log.statement.StatementFormatter;
import com.datastax.dsbulk.engine.internal.record.UnmappableRecord;
import com.datastax.dsbulk.engine.internal.statement.BulkStatement;
import com.datastax.dsbulk.engine.internal.statement.UnmappableStatement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dsbulk.executor.api.result.Result;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.CacheWriter;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;

/** */
public class LogManager implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LogManager.class);

  private final Cluster cluster;
  private final Path executionDirectory;
  private final ExecutorService executor;
  private final int maxErrors;
  private final StatementFormatter formatter;
  private final StatementFormatVerbosity verbosity;

  private final Set<Disposable> subscriptions = new HashSet<>();
  private final Set<UnicastProcessor<?>> processors = new HashSet<>();

  private final AtomicInteger errors = new AtomicInteger(0);

  private final LoadingCache<Path, PrintWriter> openFiles =
      Caffeine.newBuilder()
          .removalListener(
              (Path path, PrintWriter writer, RemovalCause cause) -> {
                if (writer != null) {
                  writer.flush();
                  writer.close();
                }
              })
          .build(
              path ->
                  new PrintWriter(
                      Files.newBufferedWriter(path, Charset.forName("UTF-8"), CREATE_NEW, WRITE)));

  private final Cache<URI, Long> locations =
      Caffeine.newBuilder()
          .writer(
              new CacheWriter<URI, Long>() {
                @Override
                public void write(URI key, Long value) {}

                @Override
                public void delete(URI base, Long line, RemovalCause cause) {
                  locationsWriter.print(base);
                  locationsWriter.print('\t');
                  locationsWriter.print(line);
                  locationsWriter.println();
                  locationsWriter.flush();
                }
              })
          .expireAfterAccess(30, SECONDS)
          .maximumSize(1000)
          .build();

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;
  private PrintWriter locationsWriter;

  public LogManager(
      Cluster cluster,
      Path executionDirectory,
      ExecutorService executor,
      int maxErrors,
      StatementFormatter formatter,
      StatementFormatVerbosity verbosity) {
    this.cluster = cluster;
    this.executionDirectory = executionDirectory;
    this.executor = executor;
    this.maxErrors = maxErrors;
    this.formatter = formatter;
    this.verbosity = verbosity;
  }

  public void init() throws IOException {
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    Files.createDirectories(executionDirectory);
    if (!Files.isDirectory(executionDirectory)) {
      throw new IllegalArgumentException(
          String.format("File %s is not a directory", executionDirectory));
    }
    if (!Files.isWritable(executionDirectory)) {
      throw new IllegalArgumentException(
          String.format("Directory %s is not writable", executionDirectory));
    }
    this.locationsWriter =
        new PrintWriter(
            Files.newBufferedWriter(
                executionDirectory.resolve("locations.txt"),
                Charset.forName("UTF-8"),
                CREATE_NEW,
                WRITE));
  }

  public Path getExecutionDirectory() {
    return executionDirectory;
  }

  @Override
  public void close() throws InterruptedException {
    executor.shutdown();
    processors.forEach(UnicastProcessor::onComplete);
    subscriptions.forEach(Disposable::dispose);
    executor.awaitTermination(1, MINUTES);
    executor.shutdownNow();
    openFiles.invalidateAll();
    openFiles.cleanUp();
    locations.invalidateAll();
    locations.cleanUp();
    if (locationsWriter != null) {
      locationsWriter.flush();
      locationsWriter.close();
      LOGGER.info(
          "Last processed locations can be found in {}",
          executionDirectory.resolve("locations.txt"));
    }
  }

  public Function<Flux<Statement>, Flux<Statement>> newRecordMapperErrorHandler() {
    UnicastProcessor<UnmappableStatement> ps = UnicastProcessor.create();
    processors.add(ps);
    subscriptions.add(
        ps.doOnNext(this::appendToDebugFile)
            .doOnNext(this::appendToBadFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (r instanceof UnmappableStatement) {
                    ps.onNext((UnmappableStatement) r);
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors)
                      throw new TooManyErrorsException(maxErrors);
                  }
                })
            .filter(r -> !(r instanceof UnmappableStatement));
  }

  public Function<Flux<Record>, Flux<Record>> newResultMapperErrorHandler() {
    UnicastProcessor<UnmappableRecord> ps = UnicastProcessor.create();
    processors.add(ps);
    subscriptions.add(
        ps.doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (r instanceof UnmappableRecord) {
                    ps.onNext((UnmappableRecord) r);
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors)
                      throw new TooManyErrorsException(maxErrors);
                  }
                })
            .filter(r -> !(r instanceof UnmappableRecord));
  }

  public Function<Flux<WriteResult>, Flux<WriteResult>> newWriteErrorHandler() {
    UnicastProcessor<WriteResult> ps = UnicastProcessor.create();
    processors.add(ps);
    subscriptions.add(
        ps.doOnNext(this::appendToDebugFile)
            .doOnNext(this::appendToBadFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (!r.isSuccess()) {
                    ps.onNext(r);
                    if (maxErrors > 0 && errors.addAndGet(delta(r.getStatement())) > maxErrors) {
                      throw new TooManyErrorsException(maxErrors);
                    }
                  }
                })
            .filter(Result::isSuccess);
  }

  public Function<Flux<ReadResult>, Flux<ReadResult>> newReadErrorHandler() {
    UnicastProcessor<ReadResult> ps = UnicastProcessor.create();
    processors.add(ps);
    subscriptions.add(
        ps.doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (!r.isSuccess()) {
                    ps.onNext(r);
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors) {
                      throw new TooManyErrorsException(maxErrors);
                    }
                  }
                })
            .filter(Result::isSuccess);
  }

  public Function<Flux<WriteResult>, Flux<WriteResult>> newLocationTracker() {
    return upstream -> upstream.doOnNext(this::incrementLocations);
  }

  @SuppressWarnings("unchecked")
  private void appendToBadFile(WriteResult result) {
    Statement statement = result.getStatement();
    if (statement instanceof BatchStatement) {
      for (Statement child : ((BatchStatement) statement).getStatements()) {
        if (child instanceof BulkStatement) {
          appendToBadFile((BulkStatement<Record>) child);
        }
      }
    } else if (statement instanceof BulkStatement) {
      appendToBadFile(((BulkStatement<Record>) statement));
    }
  }

  private void appendToBadFile(BulkStatement<Record> statement) {
    Path logFile = executionDirectory.resolve("operation.bad");
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    Object source = statement.getSource().getSource();
    printAndMaybeAddNewLine(source == null ? null : source.toString(), writer);
    writer.flush();
  }

  private void appendToDebugFile(UnmappableStatement statement) {
    Path logFile = executionDirectory.resolve("record-mapping-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendStatementInfo(statement, writer);
    statement.getError().printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToDebugFile(UnmappableRecord record) {
    Path logFile = executionDirectory.resolve("result-mapping-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    appendRecordInfo(record, writer);
    record.getError().printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToDebugFile(WriteResult result) {
    Path logFile = executionDirectory.resolve("load-errors.log");
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(ReadResult result) {
    Path logFile = executionDirectory.resolve("unload-errors.log");
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(Result result, Path logFile) {
    PrintWriter writer = openFiles.get(logFile);
    assert writer != null;
    writer.print("Statement: ");
    String format =
        formatter.format(result.getStatement(), verbosity, protocolVersion, codecRegistry);
    printAndMaybeAddNewLine(format, writer);
    result.getError().orElseThrow(IllegalStateException::new).printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  @SuppressWarnings("unchecked")
  private void incrementLocations(WriteResult result) {
    Statement statement = result.getStatement();
    if (statement instanceof BatchStatement) {
      for (Statement child : ((BatchStatement) statement).getStatements()) {
        if (child instanceof BulkStatement) {
          incrementLocations((BulkStatement<Record>) child);
        }
      }
    } else if (statement instanceof BulkStatement) {
      incrementLocations(((BulkStatement<Record>) statement));
    }
  }

  private void incrementLocations(BulkStatement<Record> statement) {
    URI location = statement.getSource().getLocation();
    long line = URIUtils.extractLine(location);
    if (line != -1) {
      try {
        URI base = URIUtils.getBaseURI(location);
        locations
            .asMap()
            .compute(
                base,
                (uri, current) -> {
                  if (current == null || line > current) {
                    return line;
                  }
                  return current;
                });
      } catch (URISyntaxException ignored) {
        // should not happen
      }
    }
  }

  private static int delta(Statement statement) {
    if (statement instanceof BatchStatement) {
      return ((BatchStatement) statement).size();
    } else {
      return 1;
    }
  }
}
