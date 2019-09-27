/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.auth;

import com.datastax.dse.driver.api.core.auth.DseGssApiAuthProviderBase;
import com.datastax.oss.driver.api.core.metadata.EndPoint;
import edu.umd.cs.findbugs.annotations.NonNull;

public class BulkGssApiAuthProvider extends DseGssApiAuthProviderBase {

  private final GssApiOptions options;

  public BulkGssApiAuthProvider(GssApiOptions options) {
    super("");
    this.options = options;
  }

  @NonNull
  @Override
  protected GssApiOptions getOptions(
      @NonNull EndPoint endPoint, @NonNull String serverAuthenticator) {
    return options;
  }
}
