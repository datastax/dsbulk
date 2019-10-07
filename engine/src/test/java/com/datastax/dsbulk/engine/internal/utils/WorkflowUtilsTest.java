/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.core.os.Native;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class WorkflowUtilsTest {

  @Test
  void should_format_custom_execution_id() {
    assertThat(WorkflowUtils.newCustomExecutionId("foo", LOAD)).isEqualTo("foo");
    assertThat(WorkflowUtils.newCustomExecutionId("%1$s", LOAD)).isEqualTo("LOAD");
    assertThat(WorkflowUtils.newCustomExecutionId("%1$s", UNLOAD)).isEqualTo("UNLOAD");
    assertThat(WorkflowUtils.newCustomExecutionId("%2$tY-%2$tm-%2$td", LOAD))
        .isEqualTo(ISO_LOCAL_DATE.format(Instant.now().atZone(UTC)));
    if (Native.isGetProcessIdAvailable()) {
      assertThat(WorkflowUtils.newCustomExecutionId("%3$s", LOAD)).matches("\\d+");
    }
  }
}
