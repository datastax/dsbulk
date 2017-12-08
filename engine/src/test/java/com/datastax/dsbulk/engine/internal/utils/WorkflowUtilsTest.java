/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.driver.core.Native;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class WorkflowUtilsTest {

  @Test
  void should_format_elapsed_time() {
    assertThat(WorkflowUtils.formatElapsed(12)).isEqualTo("12 seconds");
    assertThat(WorkflowUtils.formatElapsed(12 * 60 + 34)).isEqualTo("12 minutes and 34 seconds");
    assertThat(WorkflowUtils.formatElapsed(12 * 60 * 60 + 34 * 60 + 56))
        .isEqualTo("12 hours, 34 minutes and 56 seconds");
    assertThat(WorkflowUtils.formatElapsed(48 * 60 * 60 + 34 * 60 + 56))
        .isEqualTo("48 hours, 34 minutes and 56 seconds");
  }

  @Test
  void should_format_custom_execution_id() {
    assertThat(WorkflowUtils.newCustomExecutionId("foo", LOAD)).isEqualTo("foo");
    assertThat(WorkflowUtils.newCustomExecutionId("%1$s", LOAD)).isEqualTo("LOAD");
    assertThat(WorkflowUtils.newCustomExecutionId("%1$s", UNLOAD)).isEqualTo("UNLOAD");
    assertThat(WorkflowUtils.newCustomExecutionId("%2$tY-%2$tm-%2$td", LOAD))
        .isEqualTo(ISO_LOCAL_DATE.format(Instant.now().atZone(UTC)));
    if (Native.isGetpidAvailable()) {
      assertThat(WorkflowUtils.newCustomExecutionId("%3$s", LOAD)).matches("\\d+");
    }
  }
}
