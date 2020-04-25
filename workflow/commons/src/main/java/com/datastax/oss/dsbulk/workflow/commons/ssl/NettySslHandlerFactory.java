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
