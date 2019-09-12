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
import com.datastax.oss.driver.internal.core.ssl.SslHandlerFactory;
import io.netty.channel.Channel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import java.net.InetSocketAddress;

public class NettySslHandlerFactory implements SslHandlerFactory {

  private final SslContext sslContext;

  NettySslHandlerFactory(SslContext sslContext) {
    this.sslContext = sslContext;
  }

  @Override
  public SslHandler newSslHandler(Channel channel, EndPoint remoteEndpoint) {
    InetSocketAddress address = (InetSocketAddress) remoteEndpoint.resolve();
    return sslContext.newHandler(channel.alloc(), address.getHostName(), address.getPort());
  }

  @Override
  public void close() {
    // nothing to do
  }
}
