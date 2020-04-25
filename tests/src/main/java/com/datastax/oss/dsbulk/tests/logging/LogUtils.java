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
