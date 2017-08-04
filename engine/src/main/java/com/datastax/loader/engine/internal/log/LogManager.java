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
import com.datastax.loader.connectors.api.FailedRecord;
import com.datastax.loader.connectors.api.Record;
import com.datastax.loader.engine.internal.log.statement.StatementFormatVerbosity;
import com.datastax.loader.engine.internal.log.statement.StatementFormatter;
import com.datastax.loader.engine.internal.statement.BulkStatement;
import com.datastax.loader.engine.internal.statement.UnmappableStatement;
import com.datastax.loader.executor.api.result.Result;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalNotification;
import io.reactivex.BackpressureStrategy;
import io.reactivex.FlowableTransformer;
import io.reactivex.disposables.Disposable;
import io.reactivex.schedulers.Schedulers;
import io.reactivex.subjects.PublishSubject;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** */
public class LogManager implements AutoCloseable {

  private final Path operationDirectory;
  private final ExecutorService executor;
  private final int maxErrors;
  private final StatementFormatter formatter;
  private final StatementFormatVerbosity verbosity;
  private final Set<Disposable> disposables = new HashSet<>();
  private final AtomicInteger errors = new AtomicInteger(0);
  private final LoadingCache<Path, PrintWriter> openFiles =
      CacheBuilder.newBuilder()
          .removalListener(
              (RemovalNotification<Path, PrintWriter> notification) -> {
                PrintWriter writer = notification.getValue();
                writer.flush();
                writer.close();
              })
          .build(
              new CacheLoader<Path, PrintWriter>() {
                @Override
                public PrintWriter load(Path path) throws Exception {
                  return new PrintWriter(
                      Files.newBufferedWriter(path, Charset.forName("UTF-8"), CREATE_NEW, WRITE));
                }
              });

  private CodecRegistry codecRegistry;
  private ProtocolVersion protocolVersion;

  public LogManager(
      Path operationDirectory,
      ExecutorService executor,
      int maxErrors,
      StatementFormatter formatter,
      StatementFormatVerbosity verbosity) {
    this.operationDirectory = operationDirectory;
    this.executor = executor;
    this.maxErrors = maxErrors;
    this.formatter = formatter;
    this.verbosity = verbosity;
  }

  public void init(Cluster cluster) throws IOException {
    // TODO monitor current number of failures
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    Files.createDirectories(operationDirectory);
    if (!Files.isDirectory(operationDirectory)) {
      throw new IllegalArgumentException(
          String.format("File %s is not a directory", operationDirectory));
    }
    if (!Files.isWritable(operationDirectory)) {
      throw new IllegalArgumentException(
          String.format("Directory %s is not writable", operationDirectory));
    }
  }

  public Path getOperationDirectory() {
    return operationDirectory;
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

  public FlowableTransformer<Record, Record> newConnectorErrorHandler() {
    PublishSubject<FailedRecord> ps = PublishSubject.create();
    disposables.add(
        ps.toFlowable(BackpressureStrategy.BUFFER)
            .doOnNext(this::appendToBadFile)
            .doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.from(executor))
            .subscribe());
    return upstream ->
        upstream
            .doOnNext(
                r -> {
                  if (r instanceof FailedRecord) {
                    if (maxErrors > 0 && errors.incrementAndGet() > maxErrors)
                      throw new TooManyErrorsException(maxErrors);
                    ps.onNext((FailedRecord) r);
                  }
                })
            .filter(r -> !(r instanceof FailedRecord));
  }

  public FlowableTransformer<Statement, Statement> newMapperErrorHandler() {
    PublishSubject<UnmappableStatement> ps = PublishSubject.create();
    disposables.add(
        ps.toFlowable(BackpressureStrategy.BUFFER)
            .doOnNext(this::appendToBadFile)
            .doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.from(executor))
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

  public FlowableTransformer<Result, Result> newExecutorErrorHandler() {
    PublishSubject<Result> ps = PublishSubject.create();
    disposables.add(
        ps.toFlowable(BackpressureStrategy.BUFFER)
            .doOnNext(this::appendToBadFile)
            .doOnNext(this::appendToDebugFile)
            .subscribeOn(Schedulers.from(executor))
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

  private void appendToBadFile(Record record) throws ExecutionException, URISyntaxException {
    appendToBadFile(record.getSource());
  }

  private void appendToBadFile(BulkStatement<Record> statement)
      throws ExecutionException, URISyntaxException {
    appendToBadFile(statement.getSource());
  }

  @SuppressWarnings("unchecked")
  private void appendToBadFile(Result result) throws ExecutionException, URISyntaxException {
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

  private void appendToBadFile(Object source) throws ExecutionException {
    Path logFile = operationDirectory.resolve("operation.bad");
    PrintWriter writer = openFiles.get(logFile);
    printAndMaybeAddNewLine(source == null ? null : source.toString(), writer);
    writer.flush();
  }

  private void appendToDebugFile(FailedRecord record)
      throws ExecutionException, URISyntaxException {
    Path sourceFile = Paths.get(record.getLocation().getPath());
    Path logFile =
        operationDirectory.resolve("extract-" + sourceFile.toFile().getName() + "-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    appendRecordInfo(record, writer);
    record.getError().printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToDebugFile(UnmappableStatement statement) throws ExecutionException {
    Path logFile = operationDirectory.resolve("transform-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    appendStatementInfo(statement, writer);
    statement.getError().printStackTrace(writer);
    writer.println();
    writer.flush();
  }

  private void appendToDebugFile(Result result) throws ExecutionException {
    Path logFile = operationDirectory.resolve("load-errors.log");
    PrintWriter writer = openFiles.get(logFile);
    writer.print("Statement: ");
    String format =
        formatter.format(result.getStatement(), verbosity, protocolVersion, codecRegistry);
    printAndMaybeAddNewLine(format, writer);
    result.getError().orElseThrow(IllegalStateException::new).printStackTrace(writer);
    writer.println();
    writer.flush();
  }
}
