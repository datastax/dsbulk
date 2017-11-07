/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import com.datastax.dsbulk.tests.utils.TestAppender;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class CassLoadTest {
  @Rule
  public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1).build());

  private Logger root;
  private TestAppender appender;
  private Level oldLevel;
  private Appender<ILoggingEvent> stdout;

  @SuppressWarnings("Duplicates")
  @Before
  public void setUp() throws Exception {
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    appender = new TestAppender();
    root.addAppender(appender);
    oldLevel = root.getLevel();
    root.setLevel(Level.INFO);
    stdout = root.getAppender("STDOUT");
    root.detachAppender(stdout);
  }

  @After
  public void tearDown() throws Exception {
    root.detachAppender(appender);
    root.setLevel(oldLevel);
    root.addAppender(stdout);
  }

  @Test
  public void full_load() throws Exception {
    String[] args = {
      "load",
      "--log.directory",
      "./target",
      "-header",
      "false",
      "--connector.csv.url",
      CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm(),
      "--driver.query.consistency",
      "ONE",
      "--driver.hosts",
      EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression",
      "NONE",
      "--schema.query",
      CsvUtils.INSERT_INTO_IP_BY_COUNTRY,
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(args).run();
    List<String> errorMessages = EndToEndUtils.getErrorEventMessages(appender);
    for (String error : errorMessages) {
      System.out.println(error);
    }
    assertThat(errorMessages).isNotEmpty();
    assertThat(errorMessages.get(0))
        .contains(
            "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster");
    assertThat(errorMessages.get(1))
        .contains("The following nodes do not appear to be running DSE");
  }
}
