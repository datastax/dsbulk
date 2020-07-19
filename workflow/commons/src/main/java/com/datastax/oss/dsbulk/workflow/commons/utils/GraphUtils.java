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
import com.datastax.oss.dsbulk.workflow.api.utils.WorkflowUtils;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GraphUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowUtils.class);

  private static final Version DSE_NATIVE_GRAPH_MIN_VERSION = Version.parse("6.8.0");

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
