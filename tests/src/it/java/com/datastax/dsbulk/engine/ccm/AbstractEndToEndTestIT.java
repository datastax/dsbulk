/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.tests.ccm.CCMRule;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.Rule;

public abstract class AbstractEndToEndTestIT {
  @Rule @ClassRule public static CCMRule ccm = new CCMRule();

  public Session session;

  public final String INSERT_INTO_IP_BY_COUNTRY =
      "INSERT INTO "
          + getKeyspace()
          + ".ip_by_country "
          + "(country_code, country_name, beginning_ip_address, ending_ip_address, beginning_ip_number, ending_ip_number) "
          + "VALUES (?,?,?,?,?,?)";

  public final SimpleStatement READ_SUCCESSFUL_IP_BY_COUNTRY =
      new SimpleStatement("SELECT * FROM " + getKeyspace() + ".ip_by_country");

  public final SimpleStatement USE_KEYSPACE = new SimpleStatement("USE " + getKeyspace());

  public final SimpleStatement createKeyspace =
      new SimpleStatement(
          String.format(
              "CREATE KEYSPACE %s WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };",
              getKeyspace()));

  public String contact_point;
  public String port;

  @After
  public void clearKeyspace() {
    session.execute("DROP KEYSPACE IF EXISTS " + getKeyspace());
  }

  public String getKeyspace() {
    return "endtoendkeyspace";
  }
}
