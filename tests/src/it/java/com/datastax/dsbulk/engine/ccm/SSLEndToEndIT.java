/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.ccm;

import static com.datastax.dsbulk.tests.utils.CsvUtils.createIpByCountryTable;
import static io.netty.handler.ssl.SslProvider.OPENSSL;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.RemoteEndpointAwareJdkSSLOptions;
import com.datastax.driver.core.RemoteEndpointAwareNettySSLOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.engine.Main;
import com.datastax.dsbulk.tests.categories.LongTests;
import com.datastax.dsbulk.tests.ccm.CCMCluster;
import com.datastax.dsbulk.tests.ccm.DefaultCCMCluster;
import com.datastax.dsbulk.tests.ccm.annotations.CCMConfig;
import com.datastax.dsbulk.tests.ccm.annotations.CCMTest;
import com.datastax.dsbulk.tests.utils.CsvUtils;
import com.datastax.dsbulk.tests.utils.EndToEndUtils;
import io.netty.handler.ssl.SslContextBuilder;
import java.net.InetAddress;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.security.SecureRandom;
import java.util.LinkedList;
import java.util.List;
import javax.inject.Inject;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@CCMTest
@CCMConfig(ssl = true)
@Category(LongTests.class)
public class SSLEndToEndIT extends AbstractEndToEndTestIT {
  @Inject private static CCMCluster ccm;

  @Test
  public void full_load_unload_jdk() throws Exception {
    InetAddress cp = ccm.getInitialContactPoints().get(0);
    SSLOptions sslOptions = getSSLOptions(SslImplementation.JDK, true, true);
    Cluster cluster =
        Cluster.builder()
            .addContactPoints(cp)
            .withPort(ccm.getBinaryPort())
            .withSSL(sslOptions)
            .build();
    session = cluster.connect();
    contact_point = cp.getHostAddress().replaceFirst("^/", "");
    port = Integer.toString(ccm.getBinaryPort());
    session.execute(createKeyspace);
    session.execute(USE_KEYSPACE);

    createIpByCountryTable(session);

    /* Simple test case which attempts to load and unload data using ccm. */
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm());
    customLoadArgs.add("--schema.query");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);
    customLoadArgs.add("--driver.ssl.provider");
    customLoadArgs.add("JDK");
    customLoadArgs.add("--driver.ssl.keystore.path");
    customLoadArgs.add(getAbsoluteKeystorePath());
    customLoadArgs.add("--driver.ssl.keystore.password");
    customLoadArgs.add(DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    customLoadArgs.add("--driver.ssl.truststore.path");
    customLoadArgs.add(getAbsoluteTrustStorePath());
    customLoadArgs.add("--driver.ssl.truststore.password");
    customLoadArgs.add(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    new Main(fetchCompleteArgs("load", customLoadArgs)).run();
    validateResultSetSize(24, READ_SUCCESSFUL_IP_BY_COUNTRY);
    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url");
    customUnloadArgs.add(full_unload_dir.toString());
    customUnloadArgs.add("--schema.query");
    customUnloadArgs.add(READ_SUCCESSFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add("--driver.ssl.provider");
    customUnloadArgs.add("JDK");
    customUnloadArgs.add("--driver.ssl.keystore.path");
    customUnloadArgs.add(getAbsoluteKeystorePath());
    customUnloadArgs.add("--driver.ssl.keystore.password");
    customUnloadArgs.add(DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD);
    customUnloadArgs.add("--driver.ssl.truststore.path");
    customUnloadArgs.add(getAbsoluteTrustStorePath());
    customUnloadArgs.add("--driver.ssl.truststore.password");
    customUnloadArgs.add(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
    new Main(fetchCompleteArgs("unload", customUnloadArgs)).run();
    EndToEndUtils.validateOutputFiles(24, full_unload_output_file);
  }

  @Test
  public void full_load_unload_openssl() throws Exception {
    InetAddress cp = ccm.getInitialContactPoints().get(0);
    SSLOptions sslOptions = getSSLOptions(SslImplementation.JDK, true, true);
    Cluster cluster =
        Cluster.builder()
            .addContactPoints(cp)
            .withPort(ccm.getBinaryPort())
            .withSSL(sslOptions)
            .build();
    session = cluster.connect();
    contact_point = cp.getHostAddress().replaceFirst("^/", "");
    port = Integer.toString(ccm.getBinaryPort());
    session.execute(createKeyspace);
    session.execute(USE_KEYSPACE);

    createIpByCountryTable(session);

    /* Simple test case which attempts to load and unload data using ccm. */
    List<String> customLoadArgs = new LinkedList<>();
    customLoadArgs.add("--connector.csv.url");
    customLoadArgs.add(CsvUtils.CSV_RECORDS_UNIQUE.toExternalForm());
    customLoadArgs.add("--schema.query");
    customLoadArgs.add(INSERT_INTO_IP_BY_COUNTRY);

    customLoadArgs.add("--driver.ssl.provider");
    customLoadArgs.add("OpenSSL");
    customLoadArgs.add("--driver.ssl.openssl.keyCertChain");
    customLoadArgs.add(getAbsoluteClientCertPath());
    customLoadArgs.add("--driver.ssl.openssl.privateKey");
    customLoadArgs.add(getAbsoluteClientKeyPath());
    customLoadArgs.add("--driver.ssl.truststore.path");
    customLoadArgs.add(getAbsoluteTrustStorePath());
    customLoadArgs.add("--driver.ssl.truststore.password");
    customLoadArgs.add(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);

    new Main(fetchCompleteArgs("load", customLoadArgs)).run();
    validateResultSetSize(24, READ_SUCCESSFUL_IP_BY_COUNTRY);
    Path full_unload_dir = Paths.get("./target/full_unload_dir");
    Path full_unload_output_file = Paths.get("./target/full_unload_dir/output-000001.csv");
    EndToEndUtils.deleteIfExists(full_unload_dir);
    List<String> customUnloadArgs = new LinkedList<>();
    customUnloadArgs.add("--connector.csv.url");
    customUnloadArgs.add(full_unload_dir.toString());
    customUnloadArgs.add("--schema.query");
    customUnloadArgs.add(READ_SUCCESSFUL_IP_BY_COUNTRY.toString());
    customUnloadArgs.add("--driver.ssl.provider");
    customUnloadArgs.add("OpenSSL");
    customUnloadArgs.add("--driver.ssl.openssl.keyCertChain");
    customUnloadArgs.add(getAbsoluteClientCertPath());
    customUnloadArgs.add("--driver.ssl.openssl.privateKey");
    customUnloadArgs.add(getAbsoluteClientKeyPath());
    customUnloadArgs.add("--driver.ssl.truststore.path");
    customUnloadArgs.add(getAbsoluteTrustStorePath());
    customUnloadArgs.add("--driver.ssl.truststore.password");
    customUnloadArgs.add(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD);
    new Main(fetchCompleteArgs("unload", customUnloadArgs)).run();

    EndToEndUtils.validateOutputFiles(24, full_unload_output_file);
  }

  private void validateResultSetSize(int numOfQueries, SimpleStatement statement) {
    ResultSet set = session.execute(statement);
    List<Row> results = set.all();
    Assertions.assertThat(results.size()).isEqualTo(numOfQueries);
  }

  private String[] fetchCompleteArgs(String op, List<String> customArgs) {
    customArgs.add("--schema.mapping");
    customArgs.add(
        "0=beginning_ip_address,1=ending_ip_address,2=beginning_ip_number,3=ending_ip_number,4=country_code,5=country_name");
    return EndToEndUtils.fetchCompleteArgs(op, contact_point, port, customArgs);
  }

  private String getAbsoluteKeystorePath() {
    return getAbsoluteKeystorePath(DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PATH);
  }

  private String getAbsoluteClientCertPath() {
    return getAbsoluteKeystorePath(DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_PATH);
  }

  private String getAbsoluteClientKeyPath() {
    return getAbsoluteKeystorePath(DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_PATH);
  }

  private String getAbsoluteTrustStorePath() {
    return getAbsoluteKeystorePath(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PATH);
  }

  private String getAbsoluteKeystorePath(String path) {
    URL resource = SSLEndToEndIT.class.getResource(path);
    return resource.getPath();
  }

  @Override
  public String getKeyspace() {
    return "sslkeyspace";
  }
  /**
   * @param sslImplementation the SSL implementation to use
   * @param clientAuth whether the client should authenticate
   * @param trustingServer whether the client should trust the server's certificate
   * @return {@link com.datastax.driver.core.SSLOptions} with the given configuration for server
   *     certificate validation and client certificate authentication.
   */
  private SSLOptions getSSLOptions(
      SslImplementation sslImplementation, boolean clientAuth, boolean trustingServer)
      throws Exception {

    TrustManagerFactory tmf = null;
    if (trustingServer) {
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          this.getClass().getResourceAsStream(DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PATH),
          DefaultCCMCluster.DEFAULT_CLIENT_TRUSTSTORE_PASSWORD.toCharArray());

      tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
      tmf.init(ks);
    }

    switch (sslImplementation) {
      case JDK:
        KeyManagerFactory kmf = null;
        if (clientAuth) {
          KeyStore ks = KeyStore.getInstance("JKS");
          ks.load(
              this.getClass().getResourceAsStream(DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PATH),
              DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());

          kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
          kmf.init(ks, DefaultCCMCluster.DEFAULT_CLIENT_KEYSTORE_PASSWORD.toCharArray());
        }

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(
            kmf != null ? kmf.getKeyManagers() : null,
            tmf != null ? tmf.getTrustManagers() : null,
            new SecureRandom());

        return RemoteEndpointAwareJdkSSLOptions.builder().withSSLContext(sslContext).build();

      case NETTY_OPENSSL:
        SslContextBuilder builder =
            SslContextBuilder.forClient().sslProvider(OPENSSL).trustManager(tmf);

        if (clientAuth) {
          builder.keyManager(
              DefaultCCMCluster.DEFAULT_CLIENT_CERT_CHAIN_FILE,
              DefaultCCMCluster.DEFAULT_CLIENT_PRIVATE_KEY_FILE);
        }

        return new RemoteEndpointAwareNettySSLOptions(builder.build());
      default:
        fail("Unsupported SSL implementation: " + sslImplementation);
        return null;
    }
  }

  enum SslImplementation {
    JDK,
    NETTY_OPENSSL
  }
}
