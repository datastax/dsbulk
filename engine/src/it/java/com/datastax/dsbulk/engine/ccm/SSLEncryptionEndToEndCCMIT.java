/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_FILE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.DataStaxBulkLoader.STATUS_OK;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.assertStatus;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(ssl = true, hostnameVerification = true, auth = true)
@Tag("medium")
class SSLEncryptionEndToEndCCMIT extends EndToEndCCMITBase {

  SSLEncryptionEndToEndCCMIT(
      CCMCluster ccm,
      @SessionConfig(ssl = true, hostnameVerification = true, auth = true) CqlSession session) {
    super(ccm, session);
  }

  @BeforeAll
  void createTables() {
    createIpByCountryTable(session);
  }

  @AfterEach
  void truncateTable() {
    session.execute("TRUNCATE ip_by_country");
  }

  @Test
  void full_load_unload_jdk() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--datastax-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--datastax-java-driver.advanced.auth-provider.password");
    args.add("cassandra");
    args.add("--driver.advanced.ssl-engine-factory.class");
    args.add(DefaultSslEngineFactory.class.getSimpleName());
    args.add("--driver.advanced.ssl-engine-factory.keystore-path");
    args.add(DEFAULT_CLIENT_KEYSTORE_FILE.toString());
    args.add("--driver.advanced.ssl-engine-factory.keystore-password");
    args.add(DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    args.add("--driver.advanced.ssl-engine-factory.truststore-path");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_FILE.toString());
    args.add("--driver.advanced.ssl-engine-factory.truststore-password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--datastax-java-driver.advanced.auth-provider.username");
    args.add("cassandra");
    args.add("--datastax-java-driver.advanced.auth-provider.password");
    args.add("cassandra");
    args.add("--driver.advanced.ssl-engine-factory.class");
    args.add(DefaultSslEngineFactory.class.getSimpleName());
    args.add("--driver.advanced.ssl-engine-factory.keystore-path");
    args.add(DEFAULT_CLIENT_KEYSTORE_FILE.toString());
    args.add("--driver.advanced.ssl-engine-factory.keystore-password");
    args.add(DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    args.add("--driver.advanced.ssl-engine-factory.truststore-path");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_FILE.toString());
    args.add("--driver.advanced.ssl-engine-factory.truststore-password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_load_unload_jdk_legacy_settings() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");
    args.add("--driver.ssl.provider");
    args.add("JDK");
    args.add("--driver.ssl.keystore.path");
    args.add(DEFAULT_CLIENT_KEYSTORE_FILE.toString());
    args.add("--driver.ssl.keystore.password");
    args.add(DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    args.add("--driver.ssl.truststore.path");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_FILE.toString());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");
    args.add("--driver.ssl.provider");
    args.add("JDK");
    args.add("--driver.ssl.keystore.path");
    args.add(DEFAULT_CLIENT_KEYSTORE_FILE.toString());
    args.add("--driver.ssl.keystore.password");
    args.add(DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    args.add("--driver.ssl.truststore.path");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_FILE.toString());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_load_unload_openssl_legacy_settings() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--connector.csv.url");
    args.add(quoteJson(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");
    args.add("--driver.ssl.provider");
    args.add("OpenSSL");
    args.add("--driver.ssl.openssl.keyCertChain");
    args.add(DEFAULT_CLIENT_CERT_CHAIN_FILE.toString());
    args.add("--driver.ssl.openssl.privateKey");
    args.add(DEFAULT_CLIENT_PRIVATE_KEY_FILE.toString());
    args.add("--driver.ssl.truststore.path");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_FILE.toString());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateResultSetSize(24, "SELECT * FROM ip_by_country");
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--connector.csv.url");
    args.add(quoteJson(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(
        session
            .getKeyspace()
            .map(CqlIdentifier::asInternal)
            .orElseThrow(IllegalStateException::new));
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.auth.username");
    args.add("cassandra");
    args.add("--driver.auth.password");
    args.add("cassandra");
    args.add("--driver.ssl.provider");
    args.add("OpenSSL");
    args.add("--driver.ssl.openssl.keyCertChain");
    args.add(DEFAULT_CLIENT_CERT_CHAIN_FILE.toString());
    args.add("--driver.ssl.openssl.privateKey");
    args.add(DEFAULT_CLIENT_PRIVATE_KEY_FILE.toString());
    args.add("--driver.ssl.truststore.path");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_FILE.toString());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertStatus(status, STATUS_OK);
    validateOutputFiles(24, unloadDir);
  }
}
