/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */

package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Session;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.ccm.annotations.DSERequirement;
import com.datastax.dsbulk.tests.ccm.annotations.SessionConfig;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import com.datastax.dsbulk.tests.utils.TestAppender;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Inject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.LoggerFactory;

@CCMTest
@CCMConfig(numberOfNodes = 1)
@DSERequirement(min = "5.1")
@Category(LongTests.class)
public class MappingValidationIT extends AbstractEndToEndTestIT {
  private static final String KS = "mappingvalidation";

  private Logger root;
  private TestAppender appender;
  private Level oldLevel;

  @Inject
  @SessionConfig(useKeyspace = SessionConfig.UseKeyspaceMode.FIXED, loggedKeyspaceName = KS)
  private static Session session;

  @Inject private static Cluster cluster;

  @Override
  public String getKeyspace() {
    return KS;
  }

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
  public void duplicate_values() {
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_HEADER.toExternalForm());
    customLoadArgs.add("--schema.keyspace");
    customLoadArgs.add(getKeyspace());
    customLoadArgs.add("--schema.table");
    customLoadArgs.add("ip_by_country");
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_code");

    new Main(fetchCompleteArgs(customLoadArgs)).run();

    validateErrorMessageLogged(
        "Multiple input values in mapping resolve to column", "country_code");
  }

  @Test
  public void missing_key() throws Exception {
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_HEADER.toExternalForm());
    customLoadArgs.add("--schema.keyspace");
    customLoadArgs.add(getKeyspace());
    customLoadArgs.add("--schema.table");
    customLoadArgs.add("ip_by_country");
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number, 5=country_name");

    new Main(fetchCompleteArgs(customLoadArgs)).run();
    validateErrorMessageLogged("Missing required key column of", "country_code");
  }

  @Test
  public void extra_mapping() throws Exception {
    createIpByCountryTable(session);
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_HEADER.toExternalForm());
    customLoadArgs.add("--schema.keyspace");
    customLoadArgs.add(getKeyspace());
    customLoadArgs.add("--schema.table");
    customLoadArgs.add("ip_by_country");
    customLoadArgs.add("--schema.mapping");
    customLoadArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code, 5=country_name, 6=extra");

    new Main(fetchCompleteArgs(customLoadArgs)).run();
    validateErrorMessageLogged("doesn't match any column found in table", "extra");
  }

  private String[] fetchCompleteArgs(List<String> customArgs) {
    Host host = cluster.getMetadata().getAllHosts().iterator().next();
    String contact_point = host.getAddress().toString().replaceFirst("^/", "");
    String port = Integer.toString(host.getSocketAddress().getPort());
    return EndToEndUtils.fetchCompleteArgs("load", contact_point, port, customArgs);
  }

  @Override
  @After
  public void clearKeyspace() {
    session.execute("DROP KEYSPACE IF EXISTS " + KS);
  }

  private void validateErrorMessageLogged(String... msg) {
    List<String> errorMessages = getErrorEventMessages();
    assertThat(errorMessages).isNotEmpty();
    assertThat(errorMessages.get(0)).contains("Load workflow engine execution");
    assertThat(errorMessages.get(0)).contains("failed");
    assertThat(errorMessages.get(0)).contains(msg);
  }

  private List<String> getErrorEventMessages() {
    List<String> errorMessages = new ArrayList<>();
    List<ILoggingEvent> events = appender.getEvents();
    for (ILoggingEvent event : events) {
      if (event.getLevel().equals(Level.ERROR)) {
        errorMessages.add(event.getMessage());
      }
    }
    return errorMessages;
  }
}
