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
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.oss.driver.shaded.guava.common.annotations.VisibleForTesting;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.commons.config.ConfigUtils;
import com.datastax.oss.dsbulk.workflow.api.config.ConfigPostProcessor;
import com.typesafe.config.Config;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.io.Console;
import java.util.Map;
import java.util.Map.Entry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ConfigPostProcessor} that detects missing password settings and attempts to prompt for
 * them, if standard input is available.
 */
public class PasswordPrompter implements ConfigPostProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(PasswordPrompter.class);

  private static final Map<String, String> DEFAULT_PATHS_TO_CHECK =
      ImmutableMap.<String, String>builder()
          // Deprecated DSBulk Driver paths
          .put("dsbulk.driver.auth.username", "dsbulk.driver.auth.password")
          .put("dsbulk.driver.ssl.trustore.path", "dsbulk.driver.ssl.trustore.password")
          .put("dsbulk.driver.ssl.keystore.path", "dsbulk.driver.ssl.keystore.password")
          // New Driver paths
          .put(
              "datastax-java-driver.advanced.auth-provider.username",
              "datastax-java-driver.advanced.auth-provider.password")
          .put(
              "datastax-java-driver.advanced.ssl-engine-factory.truststore-path",
              "datastax-java-driver.advanced.ssl-engine-factory.truststore-password")
          .put(
              "datastax-java-driver.advanced.ssl-engine-factory.keystore-path",
              "datastax-java-driver.advanced.ssl-engine-factory.keystore-password")
          .build();

  private final Map<String, String> pathsToCheck;
  private final Console console;

  public PasswordPrompter() {
    this(DEFAULT_PATHS_TO_CHECK);
  }

  public PasswordPrompter(@NonNull Map<String, String> pathsToCheck) {
    this(pathsToCheck, System.console());
  }

  @VisibleForTesting
  PasswordPrompter(@NonNull Map<String, String> pathsToCheck, @Nullable Console console) {
    this.pathsToCheck = ImmutableMap.copyOf(pathsToCheck);
    this.console = console;
  }

  @Override
  public @NonNull Config postProcess(@NonNull Config config) {
    if (console != null) {
      for (Entry<String, String> entry : pathsToCheck.entrySet()) {
        String pathToCheck = entry.getKey();
        if (ConfigUtils.isPathPresentAndNotEmpty(config, pathToCheck)) {
          String pathToPrompt = entry.getValue();
          if (!config.hasPath(pathToPrompt)) {
            config = ConfigUtils.readPassword(config, pathToPrompt, console);
          }
        }
      }
    } else {
      LOGGER.debug("Standard input not available.");
    }
    return config;
  }
}
