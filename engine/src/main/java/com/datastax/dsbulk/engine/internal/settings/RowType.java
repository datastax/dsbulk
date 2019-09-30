/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

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
