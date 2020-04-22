/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.cloud;

import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.Uninterruptibles;
import com.datastax.oss.dsbulk.tests.RemoteClusterExtension;
import com.datastax.oss.dsbulk.tests.driver.factory.SessionFactory;
import java.lang.reflect.Parameter;
import java.util.List;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A manager for Cloud-enabled clusters using {@link SNIProxyServer} that helps testing with JUnit 5
 * and Cloud databases.
 */
public class SNIProxyServerExtension extends RemoteClusterExtension {

  private static final Logger LOGGER = LoggerFactory.getLogger(SNIProxyServerExtension.class);

  private static final String SNI_PROXY_SERVER = "SNI_PROXY_SERVER";

  @Override
  public boolean supportsParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Class<?> type = parameterContext.getParameter().getType();
    return type.isAssignableFrom(DefaultSNIProxyServer.class)
        || super.supportsParameter(parameterContext, extensionContext);
  }

  @Override
  public Object resolveParameter(
      ParameterContext parameterContext, ExtensionContext extensionContext)
      throws ParameterResolutionException {
    Parameter parameter = parameterContext.getParameter();
    Class<?> type = parameter.getType();
    if (type.isAssignableFrom(DefaultSNIProxyServer.class)) {
      SNIProxyServer ccm = getOrCreateSNIProxyServer(extensionContext);
      LOGGER.debug(String.format("Returning %s for parameter %s", ccm, parameter));
      return ccm;
    } else {
      return super.resolveParameter(parameterContext, extensionContext);
    }
  }

  @Override
  public void afterAll(ExtensionContext context) throws Exception {
    super.afterAll(context);
    stopSNIProxyServer(context);
  }

  @Override
  protected List<EndPoint> getContactPoints(ExtensionContext context) {
    return getOrCreateSNIProxyServer(context).getContactPoints();
  }

  @Override
  protected String getLocalDatacenter(ExtensionContext context) {
    return getOrCreateSNIProxyServer(context).getLocalDatacenter();
  }

  @Override
  protected CqlSession createSession(SessionFactory sessionFactory, ExtensionContext context) {
    SNIProxyServer proxy = getOrCreateSNIProxyServer(context);
    CqlSession session =
        sessionFactory
            .createSessionBuilder()
            .withCloudSecureConnectBundle(proxy.getSecureBundlePath())
            .withAuthCredentials("cassandra", "cassandra")
            .build();
    sessionFactory.configureSession(session);
    return session;
  }

  private SNIProxyServer getOrCreateSNIProxyServer(ExtensionContext context) {
    return context
        .getStore(TEST_NAMESPACE)
        .getOrComputeIfAbsent(
            SNI_PROXY_SERVER,
            f -> {
              int attempts = 1;
              while (true) {
                try {
                  SNIProxyServer proxy = new DefaultSNIProxyServer();
                  proxy.start();
                  return proxy;
                } catch (Exception e) {
                  if (attempts == 3) {
                    LOGGER.error("Could not start SNI proxy server, giving up", e);
                    throw e;
                  }
                  LOGGER.error("Could not start SNI proxy server, retrying", e);
                  Uninterruptibles.sleepUninterruptibly(10, SECONDS);
                }
                attempts++;
              }
            },
            SNIProxyServer.class);
  }

  private void stopSNIProxyServer(ExtensionContext context) {
    SNIProxyServer proxy =
        context.getStore(TEST_NAMESPACE).remove(SNI_PROXY_SERVER, SNIProxyServer.class);
    if (proxy != null) {
      proxy.stop();
    }
  }
}
