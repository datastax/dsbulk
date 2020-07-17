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

import com.datastax.oss.driver.shaded.guava.common.net.InetAddresses;
import edu.umd.cs.findbugs.annotations.NonNull;

public class AddressUtils {

  /**
   * Adds the default port to a contact point string, depending on whether it already has a port or
   * not.
   *
   * <p>This method tries its best to handle ambiguities with IPv6 addresses; however it is advised
   * that IPv6 addresses always be entered in full form to avoid unsolvable ambiguities.
   *
   * @param contactPoint The contact point to inspect.
   * @param defaultPort The default port to use.
   * @return The contact point with the port added, if it had no port, or the same intact contact
   *     point that was provided, if it had a port already.
   */
  @NonNull
  public static String maybeAddPortToHost(@NonNull String contactPoint, int defaultPort) {
    int colon = contactPoint.lastIndexOf(':');
    if (colon == -1) {
      // is either ipv4 or hostname without port -> add port
      return contactPoint + ':' + defaultPort;
    } else {
      String hostOrIp = contactPoint.substring(0, colon);
      if (hostOrIp.indexOf(':') != -1) {
        // is either ipv6 or ipv6:port -> disambiguate
        boolean contactPointOk = InetAddresses.isInetAddress(contactPoint);
        boolean hostOrIpOk = InetAddresses.isInetAddress(hostOrIp);
        if (!contactPointOk && hostOrIpOk) {
          // is ipv6 with port -> do nothing
          return contactPoint;
        }
        if (contactPointOk && !hostOrIpOk) {
          // is ipv6 without port -> add port
          return contactPoint + ':' + defaultPort;
        }
        try {
          Integer.parseInt(contactPoint.substring(colon + 1));
        } catch (NumberFormatException e) {
          // is ipv6 without port -> add port
          return contactPoint + ':' + defaultPort;
        }
        // other cases: ambiguous -> do nothing
      }
      // ipv4:port or hostname:port -> do nothing
      // ipv6 with ambiguous ending -> do nothing
      return contactPoint;
    }
  }
}
