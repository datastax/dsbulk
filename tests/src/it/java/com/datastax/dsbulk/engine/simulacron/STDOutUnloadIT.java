/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.simulacron;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Appender;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.codec.ConsistencyLevel;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class STDOutUnloadIT {
  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private PrintStream originalStdout;
  private ByteArrayOutputStream baos;
  private Logger root;
  private Appender<ILoggingEvent> stdoutAppender;

  @Before
  public void hijackStandardOut() {
    EndToEndUtils.setURLFactoryIfNeeded();
    originalStdout = System.out;
    baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    stdoutAppender = root.getAppender("STDOUT");
    root.detachAppender(stdoutAppender);
  }

  @After
  public void releaseStandardOut() {
    System.setOut(originalStdout);
    root.addAppender(stdoutAppender);
  }

  @Test
  public void validate_stdoutput() throws Exception {
    RequestPrime prime = EndToEndUtils.createQueryWithResultSet("SELECT * FROM ip_by_country", 24);
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.directory=./target",
      "--connector.csv.url=stdout:/",
      "--connector.csv.maxThreads=1 ",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "-header",
      "false",
      "--schema.query=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY + "",
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };

    new Main(unloadArgs);
    String out = baos.toString();
    validateQueryCount(1, ConsistencyLevel.ONE);
    EndToEndUtils.validateStringOutput(out, 24);
  }

  @SuppressWarnings("SameParameterValue")
  private void validateQueryCount(int numOfQueries, ConsistencyLevel level) {
    EndToEndUtils.validateQueryCount(
        simulacron, numOfQueries, "SELECT * FROM ip_by_country", level);
  }
}
