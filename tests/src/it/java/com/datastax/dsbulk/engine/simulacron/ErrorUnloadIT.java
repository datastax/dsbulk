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
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.SimulacronRule;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import com.datastax.dsbulk.tests.utils.TestAppender;
import com.datastax.oss.simulacron.common.cluster.ClusterSpec;
import com.datastax.oss.simulacron.common.cluster.RequestPrime;
import com.datastax.oss.simulacron.common.stubbing.Prime;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class ErrorUnloadIT {
  @Rule public SimulacronRule simulacron = new SimulacronRule(ClusterSpec.builder().withNodes(1));

  private Logger root;
  private TestAppender appender;
  private Level oldLevel;

  @Before
  public void setUp() throws Exception {
    root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    appender = new TestAppender();
    root.addAppender(appender);
    oldLevel = root.getLevel();
    root.setLevel(Level.DEBUG);
  }

  @After
  public void tearDown() throws Exception {
    root.detachAppender(appender.getName());
    root.setLevel(oldLevel);
  }

  @Test
  public void unload_existing_file() throws Exception {
    //Prior to DAT-151 this case would hang
    String existingFileName = "./target/full_unload_dir/output-000001.csv";
    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get(existingFileName);
    //cleanup anything that is there.
    EndToEndUtils.deleteIfExists(full_unload_dir);

    //Guarantee we have a duplicate file in place.
    File existingFile = new File(existingFileName);
    existingFile.getParentFile().mkdirs();
    existingFile.createNewFile();
    RequestPrime prime = EndToEndUtils.createQueryWithResultSet("SELECT * FROM ip_by_country", 24);
    simulacron.cluster().prime(new Prime(prime));
    String[] unloadArgs = {
      "unload",
      "--log.directory=./target",
      "-header",
      "false",
      "--connector.csv.url=" + full_unload_output_file.toString(),
      "--connector.csv.maxConcurrentFiles=1",
      "--driver.query.consistency=ONE",
      "--driver.hosts=" + EndToEndUtils.fetchSimulacronContactPointsForArg(simulacron),
      "--driver.protocol.compression=NONE",
      "--schema.query=" + CsvUtils.SELECT_FROM_IP_BY_COUNTRY + "",
      "--schema.mapping={0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name}"
    };
    new Main(unloadArgs).run();
    List<String> errorMessages = EndToEndUtils.getErrorEventMessages(appender);
    assertThat(errorMessages).isNotEmpty();
    assertThat(errorMessages.get(0))
        .contains(
            "Could not create CSV writer for file:/Users/gregbestland/git/datastax-loader/tests/target/full_unload_dir/output-000001.csv");
    assertThat(errorMessages.get(1))
        .contains(
            "Error writing to file:/Users/gregbestland/git/datastax-loader/tests/target/full_unload_dir/output-000001.csv");
  }
}
