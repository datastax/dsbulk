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

public enum RowType {
  REGULAR {
    @Override
    public String singular() {
      return "row";
    }

    @Override
    public String plural() {
      return "rows";
    }
  },
  VERTEX {
    @Override
    public String singular() {
      return "vertex";
    }

    @Override
    public String plural() {
      return "vertices";
    }
  },
  EDGE {
    @Override
    public String singular() {
      return "edge";
    }

    @Override
    public String plural() {
      return "edges";
    }
  };

  public abstract String singular();

  public abstract String plural();
}
