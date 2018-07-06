/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_PATH;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PATH;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_PATH;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PATH;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.escapeUserInput;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.SELECT_FROM_IP_BY_COUNTRY;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static java.nio.file.Files.createTempDirectory;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Session;
import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.driver.annotations.ClusterConfig;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(ssl = true)
@Tag("ccm")
class SSLEncryptionEndToEndCCMIT extends EndToEndCCMITBase {

  private Path unloadDir;
  private Path logDir;

  SSLEncryptionEndToEndCCMIT(CCMCluster ccm, @ClusterConfig(ssl = true) Session session) {
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

  @BeforeEach
  void setUpDirs() throws IOException {
    logDir = createTempDirectory("logs");
    unloadDir = createTempDirectory("unload");
  }

  @AfterEach
  void deleteDirs() {
    deleteDirectory(logDir);
    deleteDirectory(unloadDir);
  }

  @Test
  void full_load_unload_jdk() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.url");
    args.add(escapeUserInput(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.ssl.provider");
    args.add("JDK");
    args.add("--driver.ssl.keystore.path");
    args.add(getAbsoluteKeystorePath());
    args.add("--driver.ssl.keystore.password");
    args.add(DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    args.add("--driver.ssl.truststore.path");
    args.add(getAbsoluteTrustStorePath());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.url");
    args.add(escapeUserInput(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.ssl.provider");
    args.add("JDK");
    args.add("--driver.ssl.keystore.path");
    args.add(getAbsoluteKeystorePath());
    args.add("--driver.ssl.keystore.password");
    args.add(DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    args.add("--driver.ssl.truststore.path");
    args.add(getAbsoluteTrustStorePath());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_load_unload_openssl() throws Exception {

    List<String> args = new ArrayList<>();
    args.add("load");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.url");
    args.add(escapeUserInput(CSV_RECORDS_UNIQUE));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.ssl.provider");
    args.add("OpenSSL");
    args.add("--driver.ssl.openssl.keyCertChain");
    args.add(getAbsoluteClientCertPath());
    args.add("--driver.ssl.openssl.privateKey");
    args.add(getAbsoluteClientKeyPath());
    args.add("--driver.ssl.truststore.path");
    args.add(getAbsoluteTrustStorePath());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    int status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateResultSetSize(24, SELECT_FROM_IP_BY_COUNTRY);
    deleteDirectory(logDir);

    args = new ArrayList<>();
    args.add("unload");
    args.add("--log.directory");
    args.add(escapeUserInput(logDir));
    args.add("--connector.csv.url");
    args.add(escapeUserInput(unloadDir));
    args.add("--connector.csv.header");
    args.add("false");
    args.add("--connector.csv.maxConcurrentFiles");
    args.add("1");
    args.add("--schema.keyspace");
    args.add(session.getLoggedKeyspace());
    args.add("--schema.table");
    args.add("ip_by_country");
    args.add("--schema.mapping");
    args.add(IP_BY_COUNTRY_MAPPING_INDEXED);
    args.add("--driver.ssl.provider");
    args.add("OpenSSL");
    args.add("--driver.ssl.openssl.keyCertChain");
    args.add(getAbsoluteClientCertPath());
    args.add("--driver.ssl.openssl.privateKey");
    args.add(getAbsoluteClientKeyPath());
    args.add("--driver.ssl.truststore.path");
    args.add(getAbsoluteTrustStorePath());
    args.add("--driver.ssl.truststore.password");
    args.add(DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    status = new DataStaxBulkLoader(addContactPointAndPort(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  private static String getAbsoluteKeystorePath() {
    return getAbsoluteKeystorePath(DEFAULT_CLIENT_KEYSTORE_PATH);
  }

  private static String getAbsoluteClientCertPath() {
    return getAbsoluteKeystorePath(DEFAULT_CLIENT_CERT_CHAIN_PATH);
  }

  private static String getAbsoluteClientKeyPath() {
    return getAbsoluteKeystorePath(DEFAULT_CLIENT_PRIVATE_KEY_PATH);
  }

  private static String getAbsoluteTrustStorePath() {
    return getAbsoluteKeystorePath(DEFAULT_CLIENT_TRUSTSTORE_PATH);
  }

  private static String getAbsoluteKeystorePath(String path) {
    URL resource = SSLEncryptionEndToEndCCMIT.class.getResource(path);
    try {
      return Paths.get(resource.toURI()).normalize().toAbsolutePath().toString();
    } catch (URISyntaxException e) {
      throw new AssertionError(e);
    }
  }
}
