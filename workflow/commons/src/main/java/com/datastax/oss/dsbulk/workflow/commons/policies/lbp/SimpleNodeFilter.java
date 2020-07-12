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
package com.datastax.oss.dsbulk.workflow.commons.policies.lbp;

import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.internal.core.ContactPoints;
import com.datastax.oss.dsbulk.workflow.commons.settings.BulkDriverOption;
import com.datastax.oss.dsbulk.workflow.commons.utils.AddressUtils;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * DSSBulk's default node filter. It operates on two distinct lists: one for nodes to explicitly
 * allow, and one for nodes to explicitly deny.
 *
 * <p>If both lists are empty, all nodes are accepted. If the allow list is not empty, only nodes in
 * that list will be allowed. If the deny list is not empty, only nodes not in that list will be
 * allowed.
 *
 * <p>Usually just one of the two lists would be non-empty, but the filter also functions properly
 * when both are non empty, in which case the allow list is evaluated first and has precedence over
 * the deny list.
 *
 * <p>Nodes are resolved eagerly during startup and are not re-resolved after.
 *
 * <p>This filter is not compatible with DataStax Astra cloud deployments.
 *
 * @see BulkDriverOption#LOAD_BALANCING_POLICY_FILTER_ALLOW
 * @see BulkDriverOption#LOAD_BALANCING_POLICY_FILTER_DENY
 */
public class SimpleNodeFilter implements Predicate<Node> {

  private final Set<EndPoint> includedHosts;
  private final Set<EndPoint> excludedHosts;

  /**
   * Constructor required by the driver to create an instance of this filter.
   *
   * @param context The driver context to get the configuration from.
   * @param profileName The execution profile name; for DSBulk, this should always be the default
   *     profile name.
   */
  @SuppressWarnings("unused")
  public SimpleNodeFilter(DriverContext context, String profileName) {
    this(
        resolveHosts(context, profileName, BulkDriverOption.LOAD_BALANCING_POLICY_FILTER_ALLOW),
        resolveHosts(context, profileName, BulkDriverOption.LOAD_BALANCING_POLICY_FILTER_DENY));
  }

  public SimpleNodeFilter(Set<EndPoint> includedHosts, Set<EndPoint> excludedHosts) {
    this.includedHosts = includedHosts;
    this.excludedHosts = excludedHosts;
  }

  private static Set<EndPoint> resolveHosts(
      DriverContext context, String profileName, BulkDriverOption option) {
    DriverExecutionProfile profile = context.getConfig().getProfile(profileName);
    int defaultPort = profile.getInt(BulkDriverOption.DEFAULT_PORT);
    List<String> hosts =
        profile.getStringList(option).stream()
            .map(host -> AddressUtils.maybeAddPortToHost(host, defaultPort))
            .collect(Collectors.toList());
    return ContactPoints.merge(Collections.emptySet(), hosts, true);
  }

  @Override
  public boolean test(Node node) {
    return isIncluded(node) && isNotExcluded(node);
  }

  private boolean isIncluded(Node node) {
    for (EndPoint includedHost : includedHosts) {
      if (node.getEndPoint().equals(includedHost)) {
        return true;
      }
    }
    // an empty list means all nodes are included
    return includedHosts.isEmpty();
  }

  private boolean isNotExcluded(Node node) {
    for (EndPoint excludedHost : excludedHosts) {
      if (node.getEndPoint().equals(excludedHost)) {
        return false;
      }
    }
    return true;
  }
}
