/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.ccm;

import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.boundStatements;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.createIpByCountryTable;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.prepareInsertStatement;
import static com.datastax.dsbulk.commons.tests.utils.CsvUtils.truncateIpByCountryTable;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.utils.CsvUtils;
import com.datastax.dsbulk.connectors.cql.CqlScriptReader;
import com.datastax.dsbulk.executor.api.BulkExecutor;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import com.datastax.dsbulk.executor.api.listener.MetricsReportingExecutionListener;
import com.datastax.dsbulk.tests.ccm.CCMExtension;
import com.google.common.base.Stopwatch;
import com.google.common.io.Resources;
import com.univocity.parsers.conversions.Conversion;
import io.reactivex.Flowable;
import io.reactivex.schedulers.Schedulers;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ExtendWith(CCMExtension.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class CSVUnloadLoadCCMITBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(CSVUnloadLoadCCMITBase.class);

  private static final Conversion<String, InetAddress> INET_CONVERTER =
      new Conversion<String, InetAddress>() {

        @Override
        public InetAddress execute(String input) {
          try {
            return InetAddress.getByName(input);
          } catch (UnknownHostException e) {
            throw new RuntimeException(e);
          }
        }

        @Override
        public String revert(InetAddress input) {
          return input.getHostAddress();
        }
      };

  private final Session session;
  private final PreparedStatement insertIntoIpByCountry;
  private final PreparedStatement insertIntoCountryByIp;

  public CSVUnloadLoadCCMITBase(Session session) {
    this.session = session;
    // source table
    createIpByCountryTable(session);
    insertIntoIpByCountry = prepareInsertStatement(session);
    boundStatements(insertIntoIpByCountry).blockingSubscribe(session::execute);
    // destination table
    session.execute(
        "CREATE TABLE IF NOT EXISTS country_by_ip ("
            + "beginning_ip_address inet,"
            + "ending_ip_address inet,"
            + "beginning_ip_number bigint,"
            + "ending_ip_number bigint,"
            + "country_code varchar,"
            + "country_name varchar,"
            + "PRIMARY KEY(beginning_ip_address))");
    insertIntoCountryByIp =
        session.prepare(
            "INSERT INTO country_by_ip ("
                + "beginning_ip_address, ending_ip_address, "
                + "beginning_ip_number, ending_ip_number, "
                + "country_code, country_name) VALUES (?,?,?,?,?,?)");
  }

  @AfterAll
  void truncateTables() {
    truncateIpByCountryTable(session);
    session.execute("TRUNCATE country_by_ip");
  }

  @Test
  void should_load_cql_script() throws Exception {
    MetricsCollectingExecutionListener metrics = new MetricsCollectingExecutionListener();
    MetricsReportingExecutionListener reporter = new MetricsReportingExecutionListener(metrics);
    BulkExecutor executor = getBulkExecutor(reporter, session);

    LOGGER.info("Starting");
    Stopwatch timer = Stopwatch.createStarted();
    reporter.start(1, TimeUnit.SECONDS);

    try (CqlScriptReader cqlScriptReader = getReader("ip-by-country-sample.cql", false)) {
      executor.writeSync(cqlScriptReader.statements());
    }

    timer.stop();
    reporter.stop();
    reporter.report();
    LOGGER.info("Finished");

    assertThat(metrics.getReadsWritesTimer().getCount()).isEqualTo(500);
    assertThat(metrics.getSuccessfulReadsWritesCounter().getCount()).isEqualTo(500);
    assertThat(metrics.getFailedReadsWritesCounter().getCount()).isEqualTo(0L);
  }

  @SuppressWarnings("ConstantConditions")
  @Test
  void should_load_csv_files() {
    MetricsCollectingExecutionListener metrics = new MetricsCollectingExecutionListener();
    MetricsReportingExecutionListener reporter = new MetricsReportingExecutionListener(metrics);
    BulkExecutor executor = getBulkExecutor(reporter, session);

    StatementBatcher batcher = new StatementBatcher(session.getCluster());

    LOGGER.info("Starting");
    Stopwatch timer = Stopwatch.createStarted();
    reporter.start(1, TimeUnit.SECONDS);

    CsvUtils.csvRecords()
        .subscribeOn(Schedulers.io())
        .map(
            record ->
                insertIntoIpByCountry
                    .bind(
                        record.getString("ISO 3166 Country Code"),
                        record.getString("Country Name"),
                        record.getValue("beginning IP Address", InetAddress.class, INET_CONVERTER),
                        record.getValue("ending IP Address", InetAddress.class, INET_CONVERTER),
                        record.getLong("beginning IP Number"),
                        record.getLong("ending IP Number"))
                    .setIdempotent(true))
        .buffer(100)
        .map(batcher::batchByGroupingKey)
        .flatMap(executor::writeReactive)
        .doOnNext(
            result ->
                result
                    .getError()
                    .ifPresent(
                        e -> {
                          LOGGER.warn("Failed: " + result.getStatement(), result.getError().get());
                        }))
        .blockingSubscribe();
    timer.stop();
    reporter.stop();
    reporter.report();
    LOGGER.info("Finished");

    assertThat(session.execute("SELECT count(*) FROM ip_by_country").one().getLong(0))
        .isEqualTo(500);
  }

  @Test
  void should_read_and_write_from_cassandra() {
    MetricsCollectingExecutionListener metrics = new MetricsCollectingExecutionListener();
    MetricsReportingExecutionListener reporter = new MetricsReportingExecutionListener(metrics);
    BulkExecutor executor = getBulkExecutor(reporter, session);

    LOGGER.info("Starting");
    Stopwatch timer = Stopwatch.createStarted();
    reporter.start(1, TimeUnit.SECONDS);

    Flowable.fromPublisher(
            executor.readReactive(
                "SELECT beginning_ip_address, ending_ip_address, "
                    + "beginning_ip_number, ending_ip_number, "
                    + "country_code, country_name FROM ip_by_country"))
        .map(
            r -> {
              Row row = r.getRow().orElseThrow(IllegalStateException::new);
              return insertIntoCountryByIp.bind(
                  row.getInet("beginning_ip_address"),
                  row.getInet("ending_ip_address"),
                  row.getLong("beginning_ip_number"),
                  row.getLong("ending_ip_number"),
                  row.getString("country_code"),
                  row.getString("country_name"));
            })
        .flatMap(executor::writeReactive)
        .blockingSubscribe();

    timer.stop();
    reporter.stop();
    reporter.report();
    LOGGER.info("Finished");

    long actual = session.execute("SELECT COUNT(*) FROM country_by_ip").one().getLong(0);
    assertThat(actual)
        .isEqualTo(500)
        .isEqualTo(metrics.getReadsTimer().getCount())
        .isEqualTo(metrics.getWritesTimer().getCount());
  }

  protected abstract BulkExecutor getBulkExecutor(ExecutionListener metrics, Session session);

  @SuppressWarnings("SameParameterValue")
  private static CqlScriptReader getReader(String resource, boolean multiLine) throws IOException {
    URL url = Resources.getResource(resource);
    return new CqlScriptReader(Resources.asCharSource(url, UTF_8).openBufferedStream(), multiLine);
  }
}
