/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.ssl;

import static com.datastax.dsbulk.commons.internal.io.IOUtils.assertAccessibleFile;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import edu.umd.cs.findbugs.annotations.Nullable;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SslProvider;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.List;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

public class SslHandlerFactoryFactory {

  private static final String TRUSTSTORE = "truststore";
  private static final String KEYSTORE = "keystore";
  private static final String OPENSSL = "openssl";

  private static final String SSL = "ssl";
  private static final String SSL_PROVIDER = SSL + '.' + "provider";
  private static final String SSL_CIPHER_SUITES = SSL + '.' + "cipherSuites";
  private static final String SSL_TRUSTSTORE_PATH = SSL + '.' + TRUSTSTORE + '.' + "path";
  private static final String SSL_TRUSTSTORE_PASSWORD = SSL + '.' + TRUSTSTORE + '.' + "password";
  private static final String SSL_KEYSTORE_PATH = SSL + '.' + KEYSTORE + '.' + "path";
  private static final String SSL_KEYSTORE_PASSWORD = SSL + '.' + KEYSTORE + '.' + "password";
  private static final String SSL_TRUSTSTORE_ALGORITHM = SSL + '.' + TRUSTSTORE + '.' + "algorithm";
  private static final String SSL_OPENSSL_KEYCERTCHAIN = SSL + '.' + OPENSSL + '.' + "keyCertChain";
  private static final String SSL_OPENSSL_PRIVATE_KEY = SSL + '.' + OPENSSL + '.' + "privateKey";

  private enum SSLProvider {
    None,
    JDK,
    OpenSSL
  }

  @Nullable
  public static SslHandlerFactory createSslHandlerFactory(LoaderConfig config)
      throws GeneralSecurityException, IOException {
    SSLProvider sslProvider = config.getEnum(SSLProvider.class, SSL_PROVIDER);
    switch (sslProvider) {
      case None:
        return null;
      case JDK:
        return createJdkSslHandlerFactory(config);
      case OpenSSL:
        return createNettySslHandlerFactory(config);
      default:
        throw new BulkConfigurationException(
            String.format(
                "%s is not a valid SSL provider. Valid auth providers are %s, %s, or %s",
                sslProvider, SSLProvider.None, SSLProvider.JDK, SSLProvider.OpenSSL));
    }
  }

  private static SslHandlerFactory createJdkSslHandlerFactory(LoaderConfig config)
      throws GeneralSecurityException, IOException {
    KeyManagerFactory kmf = createKeyManagerFactory(config);
    TrustManagerFactory tmf = createTrustManagerFactory(config);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        kmf != null ? kmf.getKeyManagers() : null,
        tmf != null ? tmf.getTrustManagers() : null,
        new SecureRandom());
    List<String> cipherSuites = config.getStringList(SSL_CIPHER_SUITES);
    SslEngineFactory sslEngineFactory = new JdkSslEngineFactory(sslContext, cipherSuites);
    return new JdkSslHandlerFactory(sslEngineFactory);
  }

  private static SslHandlerFactory createNettySslHandlerFactory(LoaderConfig config)
      throws GeneralSecurityException, IOException {
    if (config.hasPath(SSL_OPENSSL_KEYCERTCHAIN) != config.hasPath(SSL_OPENSSL_PRIVATE_KEY)) {
      throw new BulkConfigurationException(
          "Settings "
              + SSL_OPENSSL_KEYCERTCHAIN
              + " and "
              + SSL_OPENSSL_PRIVATE_KEY
              + " must be provided together or not at all when using the openssl Provider");
    }
    TrustManagerFactory tmf = createTrustManagerFactory(config);
    SslContextBuilder builder =
        SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(tmf);
    if (config.hasPath(SSL_OPENSSL_KEYCERTCHAIN)) {
      Path sslOpenSslKeyCertChain = config.getPath(SSL_OPENSSL_KEYCERTCHAIN);
      Path sslOpenSslPrivateKey = config.getPath(SSL_OPENSSL_PRIVATE_KEY);
      assertAccessibleFile(sslOpenSslKeyCertChain, "OpenSSL key certificate chain file");
      assertAccessibleFile(sslOpenSslPrivateKey, "OpenSSL private key file");
      builder.keyManager(
          new BufferedInputStream(new FileInputStream(sslOpenSslKeyCertChain.toFile())),
          new BufferedInputStream(new FileInputStream(sslOpenSslPrivateKey.toFile())));
    }
    List<String> cipherSuites = config.getStringList(SSL_CIPHER_SUITES);
    if (!cipherSuites.isEmpty()) {
      builder.ciphers(cipherSuites);
    }
    SslContext sslContext = builder.build();
    return new NettySslHandlerFactory(sslContext);
  }

  private static TrustManagerFactory createTrustManagerFactory(LoaderConfig config)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    if (config.hasPath(SSL_TRUSTSTORE_PATH) != config.hasPath(SSL_TRUSTSTORE_PASSWORD)) {
      throw new BulkConfigurationException(
          "Settings "
              + SSL_TRUSTSTORE_PATH
              + ", "
              + SSL_TRUSTSTORE_PASSWORD
              + " and "
              + SSL_TRUSTSTORE_ALGORITHM
              + " must be provided together or not at all");
    }
    TrustManagerFactory tmf = null;
    if (config.hasPath(SSL_TRUSTSTORE_PATH)) {
      Path sslTrustStorePath = config.getPath(SSL_TRUSTSTORE_PATH);
      assertAccessibleFile(sslTrustStorePath, "SSL truststore file");
      String sslTrustStorePassword = config.getString(SSL_TRUSTSTORE_PASSWORD);
      String sslTrustStoreAlgorithm = config.getString(SSL_TRUSTSTORE_ALGORITHM);
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new BufferedInputStream(new FileInputStream(sslTrustStorePath.toFile())),
          sslTrustStorePassword.toCharArray());
      tmf = TrustManagerFactory.getInstance(sslTrustStoreAlgorithm);
      tmf.init(ks);
    }
    return tmf;
  }

  private static KeyManagerFactory createKeyManagerFactory(LoaderConfig config)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
          UnrecoverableKeyException {
    if (config.hasPath(SSL_KEYSTORE_PATH) != config.hasPath(SSL_KEYSTORE_PASSWORD)) {
      throw new BulkConfigurationException(
          "Settings "
              + SSL_KEYSTORE_PATH
              + ", "
              + SSL_KEYSTORE_PASSWORD
              + " and "
              + SSL_TRUSTSTORE_ALGORITHM
              + " must be provided together or not at all when using the JDK SSL Provider");
    }

    KeyManagerFactory kmf = null;
    if (config.hasPath(SSL_KEYSTORE_PATH)) {
      Path sslKeyStorePath = config.getPath(SSL_KEYSTORE_PATH);
      assertAccessibleFile(sslKeyStorePath, "SSL keystore file");
      String sslKeyStorePassword = config.getString(SSL_KEYSTORE_PASSWORD);
      String sslTrustStoreAlgorithm = config.getString(SSL_TRUSTSTORE_ALGORITHM);
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new BufferedInputStream(new FileInputStream(sslKeyStorePath.toFile())),
          sslKeyStorePassword.toCharArray());

      kmf = KeyManagerFactory.getInstance(sslTrustStoreAlgorithm);
      kmf.init(ks, sslKeyStorePassword.toCharArray());
    }
    return kmf;
  }
}
