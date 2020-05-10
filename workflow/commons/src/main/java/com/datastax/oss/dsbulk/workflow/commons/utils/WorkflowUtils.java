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
package com.datastax.oss.dsbulk.workflow.commons.utils;

import com.datastax.dse.driver.api.core.metadata.DseNodeProperties;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.dsbulk.commons.utils.PlatformUtils;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowUtils {

  private static final UUID BULK_LOADER_NAMESPACE =
      UUID.fromString("2505c745-cedf-4714-bcab-0d580270ed95");

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowUtils.class);

  /**
   * The threshold, in number of resources to read or write, that triggers a thread-per-core
   * optimization.
   *
   * <p>This threshold actually varies a bit depending on the dataset to load or unload, but it
   * generally starts to be advantageous when the number of resources is &gt;= 1/4 of the number of
   * available cores.
   */
  public static final int TPC_THRESHOLD =
      Math.max(4, Runtime.getRuntime().availableProcessors() / 4);

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  private static final Version DSE_NATIVE_GRAPH_MIN_VERSION = Version.parse("6.8.0");

  public static String newDefaultExecutionId(@NonNull String operationTitle) {
    return operationTitle.toUpperCase()
        + "_"
        + DEFAULT_TIMESTAMP_PATTERN.format(PlatformUtils.now());
  }

  public static String newCustomExecutionId(
      @NonNull String template, @NonNull String operationTitle) {
    try {
      // Accepted parameters:
      // 1 : the operation title
      // 2 : the current time
      // 3 : the JVM process PID, if available
      String executionId =
          String.format(template, operationTitle, PlatformUtils.now(), PlatformUtils.pid());
      if (executionId.isEmpty()) {
        throw new IllegalStateException("Generated execution ID is empty.");
      }
      return executionId;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Could not generate execution ID with template: '" + template + "': " + e.getMessage(),
          e);
    }
  }

  public static UUID clientId(String executionId) {
    return Uuids.nameBased(BULK_LOADER_NAMESPACE, executionId);
  }

  public static void checkGraphCompatibility(Session session) {
    Collection<Node> nodes = session.getMetadata().getNodes().values();
    List<Node> nonGraphNodes =
        nodes.stream()
            .filter(
                node ->
                    !node.getExtras().containsKey(DseNodeProperties.DSE_VERSION)
                        || ((Version) node.getExtras().get(DseNodeProperties.DSE_VERSION))
                                .compareTo(Objects.requireNonNull(DSE_NATIVE_GRAPH_MIN_VERSION))
                            < 0)
            .collect(Collectors.toList());
    if (!nonGraphNodes.isEmpty()) {
      LOGGER.error(
          "Incompatible cluster detected. Graph functionality is only compatible with DSE {} or higher.",
          DSE_NATIVE_GRAPH_MIN_VERSION);
      LOGGER.error(
          "The following nodes do not appear to be running DSE {} or higher:",
          DSE_NATIVE_GRAPH_MIN_VERSION);
      for (Node node : nonGraphNodes) {
        LOGGER.error(node.toString());
      }
      throw new IllegalStateException("Graph operations not available due to incompatible cluster");
    }
  }
}
