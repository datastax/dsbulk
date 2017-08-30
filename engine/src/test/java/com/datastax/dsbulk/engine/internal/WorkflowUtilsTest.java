/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class WorkflowUtilsTest {

  @Test
  public void should_format_elapsed_time() throws Exception {
    assertThat(WorkflowUtils.formatElapsed(12)).isEqualTo("12 seconds");
    assertThat(WorkflowUtils.formatElapsed(12 * 60 + 34)).isEqualTo("12 minutes and 34 seconds");
    assertThat(WorkflowUtils.formatElapsed(12 * 60 * 60 + 34 * 60 + 56))
        .isEqualTo("12 hours, 34 minutes and 56 seconds");
    assertThat(WorkflowUtils.formatElapsed(48 * 60 * 60 + 34 * 60 + 56))
        .isEqualTo("48 hours, 34 minutes and 56 seconds");
  }
}
