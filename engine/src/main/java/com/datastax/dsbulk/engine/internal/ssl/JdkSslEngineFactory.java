/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.ssl.SslEngineFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;

public class JdkSslEngineFactory implements SslEngineFactory {

  private final SSLContext sslContext;
  private final String[] cipherSuites;

  public JdkSslEngineFactory(@NonNull SSLContext sslContext, @NonNull List<String> cipherSuites) {
    this.sslContext = sslContext;
    this.cipherSuites = cipherSuites.isEmpty() ? null : cipherSuites.toArray(new String[0]);
  }

  @NonNull
  @Override
  public SSLEngine newSslEngine(@NonNull EndPoint remoteEndpoint) {
    SSLEngine engine;
    SocketAddress remoteAddress = remoteEndpoint.resolve();
    if (remoteAddress instanceof InetSocketAddress) {
      InetSocketAddress socketAddress = (InetSocketAddress) remoteAddress;
      engine = sslContext.createSSLEngine(socketAddress.getHostName(), socketAddress.getPort());
    } else {
      engine = sslContext.createSSLEngine();
    }
    engine.setUseClientMode(true);
    if (cipherSuites != null) {
      engine.setEnabledCipherSuites(cipherSuites);
    }
    return engine;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
