/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.logging;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.URL;
import org.slf4j.LoggerFactory;

public class LogUtils {

  public static void resetLogbackConfiguration() throws JoranException {
    URL resource = ClassLoader.getSystemResource("logback-test.xml");
    if (resource == null) {
      resource = ClassLoader.getSystemResource("logback.xml");
    }
    resetLogbackConfiguration(resource);
  }

  public static void resetLogbackConfiguration(@NonNull String resourceName) throws JoranException {
    URL resource = ClassLoader.getSystemResource(resourceName);
    resetLogbackConfiguration(resource);
  }

  public static void resetLogbackConfiguration(@Nullable URL config) throws JoranException {
    LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
    JoranConfigurator configurator = new JoranConfigurator();
    configurator.setContext(context);
    context.reset();
    if (config != null) {
      configurator.doConfigure(config);
    }
  }
}
