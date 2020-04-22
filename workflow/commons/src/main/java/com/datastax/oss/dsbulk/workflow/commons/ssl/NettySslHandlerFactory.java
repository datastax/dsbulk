/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.ssl;

import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

public class NettySslHandlerFactory implements SslHandlerFactory {

  private final SslContext sslContext;

  NettySslHandlerFactory(@NonNull SslContext sslContext) {
    this.sslContext = sslContext;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
    InetSocketAddress address = (InetSocketAddress) remoteEndpoint.resolve();
    SslHandler sslHandler =
        sslContext.newHandler(channel.alloc(), address.getHostName(), address.getPort());
    // enable hostname verification
    SSLEngine sslEngine = sslHandler.engine();
    SSLParameters sslParameters = sslEngine.getSSLParameters();
    sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
    sslEngine.setSSLParameters(sslParameters);
    return sslHandler;
  }

  @Override
  public void close() {
    // nothing to do
  }
}
