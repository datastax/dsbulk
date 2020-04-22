/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.settings;

import com.datastax.oss.driver.api.core.config.DriverOption;
import com.datastax.oss.dsbulk.workflow.commons.policies.retry.MultipleRetryPolicy;
import edu.umd.cs.findbugs.annotations.NonNull;

public enum BulkDriverOption implements DriverOption {

  /**
   * The default port to use when {@link
   * com.datastax.oss.driver.api.core.config.DefaultDriverOption#CONTACT_POINTS} contains hosts
   * without ports.
   */
  DEFAULT_PORT("basic.default-port"),

  /** The maximum number of attemps to retry, when using {@link MultipleRetryPolicy}. */
  RETRY_POLICY_MAX_RETRIES("advanced.retry-policy.max-retries"),
  ;

  private final String path;

  BulkDriverOption(String path) {
    this.path = path;
  }

  @NonNull
  @Override
  public String getPath() {
    return path;
  }
}
