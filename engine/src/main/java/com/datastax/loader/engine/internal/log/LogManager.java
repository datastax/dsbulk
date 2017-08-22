/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.log;

import static com.datastax.loader.engine.internal.log.LogUtils.appendRecordInfo;
import static com.datastax.loader.engine.internal.log.LogUtils.appendStatementInfo;
import static com.datastax.loader.engine.internal.log.LogUtils.printAndMaybeAddNewLine;
import static java.nio.file.StandardOpenOption.CREATE_NEW;
import static java.nio.file.StandardOpenOption.WRITE;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.loader.engine.internal.log.statement.StatementFormatter;
import com.datastax.loader.engine.internal.record.UnmappableRecord;
import com.datastax.loader.engine.internal.statement.BulkStatement;
import com.datastax.loader.engine.internal.statement.UnmappableStatement;
import com.datastax.loader.executor.api.result.ReadResult;
import com.datastax.loader.executor.api.result.Result;
import com.datastax.loader.executor.api.result.WriteResult;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import com.github.benmanes.caffeine.cache.RemovalCause;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.UnicastProcessor;
import reactor.core.scheduler.Schedulers;

/** */
public class LogManager implements AutoCloseable {

  private final Cluster cluster;
  private final Path executionDirectory;
  private final ExecutorService executor;
  private final int maxErrors;
  private final StatementFormatter formatter;
  private final StatementFormatVerbosity verbosity;
  private final Set<Disposable> disposables = new HashSet<>();
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

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;

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
  }

  public Path getExecutionDirectory() {
    return executionDirectory;
  }

  @Override
  public void close() throws InterruptedException {
    executor.shutdown();
    executor.awaitTermination(1, TimeUnit.MINUTES);
    // FIXME it seems that sometimes an item might get lost when the disposable is canceled too soon
    disposables.forEach(Disposable::dispose);
    openFiles.invalidateAll();
    executor.shutdownNow();
  }

  public Function<Flux<Statement>, Flux<Statement>> newRecordMapperErrorHandler() {
    UnicastProcessor<UnmappableStatement> ps = UnicastProcessor.create();
    disposables.add(
        ps.doOnNext(this::appendToDebugFile)
            .doOnNext(this::appendToBadFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (r instanceof UnmappableStatement) {
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors)
                      throw new TooManyErrorsException(maxErrors);
                    ps.onNext((UnmappableStatement) r);
                  }
                })
            .filter(r -> !(r instanceof UnmappableStatement));
  }

  public Function<Flux<Record>, Flux<Record>> newResultMapperErrorHandler() {
    UnicastProcessor<UnmappableRecord> ps = UnicastProcessor.create();
    disposables.add(
        ps.doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (r instanceof UnmappableRecord) {
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors)
                      throw new TooManyErrorsException(maxErrors);
                    ps.onNext((UnmappableRecord) r);
                  }
                })
            .filter(r -> !(r instanceof UnmappableRecord));
  }

  public Function<Flux<WriteResult>, Flux<WriteResult>> newWriteErrorHandler() {
    UnicastProcessor<WriteResult> ps = UnicastProcessor.create();
    disposables.add(
        ps.doOnNext(this::appendToDebugFile)
            .doOnNext(this::appendToBadFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (!r.isSuccess()) {
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors)
                      throw new TooManyErrorsException(maxErrors);
                    ps.onNext(r);
                  }
                })
            .filter(Result::isSuccess);
  }

  public Function<Flux<ReadResult>, Flux<ReadResult>> newReadErrorHandler() {
    UnicastProcessor<ReadResult> ps = UnicastProcessor.create();
    disposables.add(
        ps.doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.fromExecutor(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (!r.isSuccess()) {
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors) {
                      throw new TooManyErrorsException(maxErrors);
                    }
                    ps.onNext(r);
                  }
                })
            .filter(Result::isSuccess);
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
    Path logFile = executionDirectory.resolve("write-errors.log");
    appendToDebugFile(result, logFile);
  }

  private void appendToDebugFile(ReadResult result) {
    Path logFile = executionDirectory.resolve("read-errors.log");
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
}
