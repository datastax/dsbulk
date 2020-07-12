/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.workflow.commons.ssl;

import static com.datastax.oss.dsbulk.io.IOUtils.assertAccessibleFile;

import com.datastax.oss.driver.api.core.ssl.ProgrammaticSslEngineFactory;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import com.datastax.oss.driver.internal.core.ssl.JdkSslHandlerFactory;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import com.datastax.oss.dsbulk.config.ConfigUtils;
import com.typesafe.config.Config;
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

  @Nullable
  public static SslHandlerFactory createSslHandlerFactory(Config config)
      throws GeneralSecurityException, IOException {
    String sslProvider = config.getString("provider");
    switch (sslProvider.toLowerCase()) {
      case "none":
        return null;
      case "jdk":
        return createJdkSslHandlerFactory(config);
      case "openssl":
        return createNettySslHandlerFactory(config);
      default:
        throw new IllegalArgumentException(
            String.format(
                "Invalid value for dsbulk.driver.ssl.provider, expecting None, JDK, or OpenSSL, got: '%s'",
                sslProvider));
    }
  }

  private static SslHandlerFactory createJdkSslHandlerFactory(Config config)
      throws GeneralSecurityException, IOException {
    KeyManagerFactory kmf = createKeyManagerFactory(config);
    TrustManagerFactory tmf = createTrustManagerFactory(config);
    SSLContext sslContext = SSLContext.getInstance("TLS");
    sslContext.init(
        kmf != null ? kmf.getKeyManagers() : null,
        tmf != null ? tmf.getTrustManagers() : null,
        new SecureRandom());
    List<String> cipherSuites = config.getStringList("cipherSuites");
    SslEngineFactory sslEngineFactory =
        new ProgrammaticSslEngineFactory(
            sslContext, cipherSuites.isEmpty() ? null : cipherSuites.toArray(new String[0]), true);
    return new JdkSslHandlerFactory(sslEngineFactory);
  }

  private static SslHandlerFactory createNettySslHandlerFactory(Config config)
      throws GeneralSecurityException, IOException {
    if (config.hasPath("openssl.keyCertChain") != config.hasPath("openssl.privateKey")) {
      throw new IllegalArgumentException(
          "Settings "
              + "dsbulk.driver.ssl.openssl.keyCertChain"
              + " and "
              + "dsbulk.driver.ssl.openssl.privateKey"
              + " must be provided together or not at all when using the OpenSSL provider");
    }
    TrustManagerFactory tmf = createTrustManagerFactory(config);
    SslContextBuilder builder =
        SslContextBuilder.forClient().sslProvider(SslProvider.OPENSSL).trustManager(tmf);
    if (config.hasPath("openssl.keyCertChain")) {
      Path sslOpenSslKeyCertChain = ConfigUtils.getPath(config, "openssl.keyCertChain");
      Path sslOpenSslPrivateKey = ConfigUtils.getPath(config, "openssl.privateKey");
      assertAccessibleFile(sslOpenSslKeyCertChain, "OpenSSL key certificate chain file");
      assertAccessibleFile(sslOpenSslPrivateKey, "OpenSSL private key file");
      builder.keyManager(
          new BufferedInputStream(new FileInputStream(sslOpenSslKeyCertChain.toFile())),
          new BufferedInputStream(new FileInputStream(sslOpenSslPrivateKey.toFile())));
    }
    List<String> cipherSuites = config.getStringList("cipherSuites");
    if (!cipherSuites.isEmpty()) {
      builder.ciphers(cipherSuites);
    }
    SslContext sslContext = builder.build();
    return new NettySslHandlerFactory(sslContext);
  }

  private static TrustManagerFactory createTrustManagerFactory(Config config)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException {
    if (config.hasPath("truststore.path") != config.hasPath("truststore.password")) {
      throw new IllegalArgumentException(
          "Settings "
              + "dsbulk.driver.ssl.truststore.path"
              + ", "
              + "dsbulk.driver.ssl.truststore.password"
              + " and "
              + "dsbulk.driver.ssl.truststore.algorithm"
              + " must be provided together or not at all");
    }
    TrustManagerFactory tmf = null;
    if (config.hasPath("truststore.path")) {
      Path sslTrustStorePath = ConfigUtils.getPath(config, "truststore.path");
      assertAccessibleFile(sslTrustStorePath, "SSL truststore file");
      String sslTrustStorePassword = config.getString("truststore.password");
      String sslTrustStoreAlgorithm = config.getString("truststore.algorithm");
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new BufferedInputStream(new FileInputStream(sslTrustStorePath.toFile())),
          sslTrustStorePassword.toCharArray());
      tmf = TrustManagerFactory.getInstance(sslTrustStoreAlgorithm);
      tmf.init(ks);
    }
    return tmf;
  }

  private static KeyManagerFactory createKeyManagerFactory(Config config)
      throws KeyStoreException, IOException, NoSuchAlgorithmException, CertificateException,
          UnrecoverableKeyException {
    if (config.hasPath("keystore.path") != config.hasPath("keystore.password")) {
      throw new IllegalArgumentException(
          "Settings "
              + "dsbulk.driver.ssl.keystore.path"
              + ", "
              + "dsbulk.driver.ssl.keystore.password"
              + " and "
              + "dsbulk.driver.ssl.keystore.algorithm"
              + " must be provided together or not at all when using the JDK SSL provider");
    }
    KeyManagerFactory kmf = null;
    if (config.hasPath("keystore.path")) {
      Path sslKeyStorePath = ConfigUtils.getPath(config, "keystore.path");
      assertAccessibleFile(sslKeyStorePath, "SSL keystore file");
      String sslKeyStorePassword = config.getString("keystore.password");
      String sslKeyStoreAlgorithm = config.getString("keystore.algorithm");
      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          new BufferedInputStream(new FileInputStream(sslKeyStorePath.toFile())),
          sslKeyStorePassword.toCharArray());

      kmf = KeyManagerFactory.getInstance(sslKeyStoreAlgorithm);
      kmf.init(ks, sslKeyStorePassword.toCharArray());
    }
    return kmf;
  }
}
