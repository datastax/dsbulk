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
package com.datastax.oss.dsbulk.workflow.commons.metrics.jmx;

import com.codahale.metrics.jmx.ObjectNameFactory;
import com.datastax.oss.dsbulk.workflow.commons.utils.JMXUtils;
import java.util.StringTokenizer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

public class BulkLoaderObjectNameFactory implements ObjectNameFactory {

  private final String executionId;

  public BulkLoaderObjectNameFactory(String executionId) {
    this.executionId = executionId;
  }

  @Override
  public ObjectName createName(String type, String domain, String name) {
    try {
      StringBuilder sb =
          new StringBuilder(domain)
              .append(":executionId=")
              .append(JMXUtils.quoteJMXIfNecessary(executionId))
              .append(',');
      // DSBulk metrics hierarchy is defined with / but driver metrics hierarchy is defined with
      // dots. We need to normalize them now.
      name = name.replace('.', '/');
      StringTokenizer tokenizer = new StringTokenizer(name, "/");
      int i = 1;
      while (tokenizer.hasMoreTokens()) {
        String token = tokenizer.nextToken();
        if (tokenizer.hasMoreTokens()) {
          sb.append("level").append(i++);
        } else {
          sb.append("name");
        }
        sb.append('=').append(JMXUtils.quoteJMXIfNecessary(token));
        if (tokenizer.hasMoreTokens()) {
          sb.append(',');
        }
      }
      return ObjectName.getInstance(sb.toString());
    } catch (MalformedObjectNameException e) {
      throw new RuntimeException(e);
    }
  }
}
