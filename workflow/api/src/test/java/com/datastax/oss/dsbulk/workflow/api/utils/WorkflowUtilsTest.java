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
package com.datastax.oss.dsbulk.workflow.api.utils;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.internal.core.os.Native;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class WorkflowUtilsTest {

  @Test
  void should_format_custom_execution_id() {
    assertThat(WorkflowUtils.newCustomExecutionId("foo", "LOAD")).isEqualTo("foo");
    assertThat(WorkflowUtils.newCustomExecutionId("%1$s", "LOAD")).isEqualTo("LOAD");
    assertThat(WorkflowUtils.newCustomExecutionId("%1$s", "UNLOAD")).isEqualTo("UNLOAD");
    assertThat(WorkflowUtils.newCustomExecutionId("%2$tY-%2$tm-%2$td", "LOAD"))
        .isEqualTo(ISO_LOCAL_DATE.format(Instant.now().atZone(UTC)));
    if (Native.isGetProcessIdAvailable()) {
      assertThat(WorkflowUtils.newCustomExecutionId("%3$s", "LOAD")).matches("\\d+");
    }
  }
}
