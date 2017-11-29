/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.internal.assertions.CommonsAssertions.assertThat;
import static com.datastax.dsbulk.tests.utils.CsvUtils.CSV_RECORDS_HEADER;
import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static org.slf4j.event.Level.ERROR;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.internal.logging.LogCapture;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptingExtension;
import com.datastax.dsbulk.commons.internal.logging.LogInterceptor;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
@Tag("ccm")
class MappingValidationEndToEndCCMIT extends EndToEndCCMITBase {

  private final LogInterceptor interceptor;

  MappingValidationEndToEndCCMIT(
      CCMCluster ccm,
      Session session,
      @LogCapture(value = Main.class, level = ERROR) LogInterceptor interceptor) {
    super(ccm, session);
    this.interceptor = interceptor;
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
  }

  @Test
  void duplicate_values() throws IOException {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_HEADER.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_code");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateErrorMessageLogged(
        "Multiple input values in mapping resolve to column", "country_code");
  }

  @Test
  void missing_key() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_HEADER.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number, 5=country_name");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateErrorMessageLogged("Missing required key column of", "country_code");
  }

  @Test
  void extra_mapping() throws Exception {
    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(Files.createTempDirectory("test").toString());
    args.add("--connector.csv.url");
    args.add(CSV_RECORDS_HEADER.toExternalForm());
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code, 5=country_name, 6=extra");

    int status = new Main(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateErrorMessageLogged("doesn't match any column found in table", "extra");
  }

  private void validateErrorMessageLogged(String... msg) {
    assertThat(interceptor)
        .hasMessageContaining("Load workflow engine execution")
        .hasMessageContaining("failed");
    for (String s : msg) {
      assertThat(interceptor).hasMessageContaining(s);
    }
  }
}
