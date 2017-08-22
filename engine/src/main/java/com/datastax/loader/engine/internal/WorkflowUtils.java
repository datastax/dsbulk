/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.loader.engine.WorkflowType;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Random;

public class WorkflowUtils {

  public static String newExecutionId(WorkflowType workflowType) {
    return workflowType
        + "_"
        + DateTimeFormatter.ofPattern("uuuu_MM_dd_HH_mm_ss_nnnnnnnnn")
            .format(
                ZonedDateTime.ofInstant(
                        Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of("UTC"))
                    .with(ChronoField.NANO_OF_SECOND, new Random().nextInt(1_000_000_000)));
  }

  public static String formatElapsed(long seconds) {
    long hr = SECONDS.toHours(seconds);
    long min = SECONDS.toMinutes(seconds - HOURS.toSeconds(hr));
    long sec = seconds - HOURS.toSeconds(hr) - MINUTES.toSeconds(min);
    if (hr > 0) {
      return String.format("%d hours, %d minutes and %d seconds", hr, min, sec);
    } else if (min > 0) {
      return String.format("%d minutes and %d seconds", min, sec);
    } else {
      return String.format("%d seconds", sec);
    }
  }
}
