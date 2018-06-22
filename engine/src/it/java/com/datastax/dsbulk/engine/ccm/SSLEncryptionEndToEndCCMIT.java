/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DDAC;
import static com.datastax.dsbulk.commons.tests.ccm.CCMCluster.Type.DSE;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_PATH;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PATH;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_PATH;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD;
import static com.datastax.dsbulk.commons.tests.ccm.DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PATH;
import static com.datastax.dsbulk.commons.tests.utils.FileUtils.deleteDirectory;
import static com.datastax.dsbulk.commons.tests.utils.StringUtils.quoteJson;
import static com.datastax.dsbulk.engine.tests.utils.CsvUtils.CSV_RECORDS_UNIQUE;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.IP_BY_COUNTRY_MAPPING_INDEXED;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.createIpByCountryTable;
import static com.datastax.dsbulk.engine.tests.utils.EndToEndUtils.validateOutputFiles;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.dsbulk.commons.tests.ccm.CCMCluster;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.commons.tests.ccm.annotations.CCMRequirements;
import com.datastax.dsbulk.commons.tests.driver.annotations.SessionConfig;
import com.datastax.dsbulk.engine.DataStaxBulkLoader;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@CCMConfig(ssl = true)
@Tag("medium")
@CCMRequirements(compatibleTypes = {DSE, DDAC})
class SSLEncryptionEndToEndCCMIT extends EndToEndCCMITBase {

  SSLEncryptionEndToEndCCMIT(CCMCluster ccm, @SessionConfig(ssl = true) CqlSession session) {
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

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
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

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
    validateOutputFiles(24, unloadDir);
  }

  @Test
  void full_load_unload_openssl() throws Exception {

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

    int status = new DataStaxBulkLoader(addCommonSettings(args)).run();
    assertThat(status).isZero();
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

    status = new DataStaxBulkLoader(addCommonSettings(args)).run();
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
