/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.metrics;

import static com.datastax.dsbulk.engine.internal.utils.StringUtils.leftPad;
import static java.lang.Math.max;
import static java.lang.String.format;
import static org.fusesource.jansi.Ansi.ansi;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.Timer;
import com.datastax.dsbulk.engine.internal.utils.HelpUtils;
import com.datastax.dsbulk.executor.api.listener.MetricsCollectingExecutionListener;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.Locale;
import java.util.SortedMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.fusesource.jansi.Ansi;
import org.jetbrains.annotations.NotNull;

/**
 * An {@link ScheduledReporter} that reports useful metrics about ongoing operations to the standard
 * error channel, using ANSI escape codes. It relies on a delegate {@link
 * MetricsCollectingExecutionListener} as its source of metrics.
 */
public class ConsoleReporter extends ScheduledReporter {

  private static final String REPORTER_NAME = "console-reporter";

  private static final double BYTES_PER_KB = 1024;
  private static final double BYTES_PER_MB = BYTES_PER_KB * BYTES_PER_KB;
  private static final int LINE_LENGTH = HelpUtils.getLineLength();

  private final long expectedTotal;
  private final Supplier<Long> total;
  private final Supplier<Long> failed;
  private final Timer timer;
  private final Meter bytes;
  private final Histogram batchSizes;
  private final InterceptingPrintStream stderr;
  private final String rateUnit;
  private final String durationUnit;

  ConsoleReporter(
      MetricRegistry registry,
      Supplier<Long> total,
      Supplier<Long> failed,
      Timer timer,
      Meter bytes,
      Histogram batchSizes,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      long expectedTotal,
      ScheduledExecutorService scheduler) {
    super(registry, REPORTER_NAME, (name, metric) -> true, rateUnit, durationUnit, scheduler);
    this.total = total;
    this.failed = failed;
    this.timer = timer;
    this.bytes = bytes;
    this.batchSizes = batchSizes;
    this.expectedTotal = expectedTotal;
    this.rateUnit = getAbbreviatedUnit(rateUnit);
    this.durationUnit = getAbbreviatedUnit(durationUnit);
    // This reporter expects System.err to be an ANSI-ready stream, see LogSettings.
    stderr = new InterceptingPrintStream(System.err);
    System.setErr(stderr);
  }

  @Override
  public void report(
      SortedMap<String, Gauge> gauges,
      SortedMap<String, Counter> counters,
      SortedMap<String, Histogram> histograms,
      SortedMap<String, Meter> meters,
      SortedMap<String, Timer> timers) {

    // NOTE: when modifying escape sequences, make sure
    // that they are supported on Windows.

    // compute values
    Ansi header = ansi();
    Ansi message = ansi();

    // totals
    long total = this.total.get();
    long failed = this.failed.get();
    String totalStr = format("%,d", total);
    String failedStr = format("%,d", failed);
    int totalLength = max("total".length(), totalStr.length());
    int failedLength = max("failed".length(), failedStr.length());
    header = header.a(leftPad("total", totalLength)).a(" | ").a(leftPad("failed", failedLength));
    message =
        message
            .fgCyan()
            .a(leftPad(totalStr, totalLength))
            .reset()
            .a(" | ")
            .fgCyan()
            .a(leftPad(failedStr, failedLength));

    // percentage achieved
    if (expectedTotal != -1) {
      float achieved = (float) total / (float) expectedTotal * 100f;
      String achievedStr = format("%,.0f%%", achieved);
      int achievedLength = max("achieved".length(), achievedStr.length());
      header = header.a(" | ").a(leftPad("achieved", achievedLength));
      message = message.reset().a(" | ").fgCyan().a(leftPad(achievedStr, achievedLength));
    }

    // throughput
    if (header.toString().length() < LINE_LENGTH) {
      double throughput = timer.getMeanRate();
      double rowsPerUnit = convertRate(throughput);
      double bytes = this.bytes.getMeanRate();
      double mbPerUnit = convertRate(bytes / BYTES_PER_MB);
      double kbPerRow = throughput == 0 ? 0 : (bytes / BYTES_PER_KB) / throughput;
      String rowsPerUnitStr = format("%,.0f", rowsPerUnit);
      String mbPerUnitStr = format("%,.2f", mbPerUnit);
      String kbPerRowStr = format("%,.2f", kbPerRow);
      String rowsPerUnitLabel = "rows/" + rateUnit;
      String mbPerRateUnitLabel = "mb/" + rateUnit;
      int rowsPerUnitLength = max(rowsPerUnitLabel.length(), rowsPerUnitStr.length());
      int mbPerUnitLength = max(mbPerRateUnitLabel.length(), mbPerUnitStr.length());
      int kbPerRowLength = max("kb/row".length(), kbPerRowStr.length());
      header =
          header
              .a(" | ")
              .a(leftPad(rowsPerUnitLabel, rowsPerUnitLength))
              .a(" | ")
              .a(leftPad(mbPerRateUnitLabel, mbPerUnitLength))
              .a(" | ")
              .a(leftPad("kb/row", kbPerRowLength));
      message =
          message
              .reset()
              .a(" | ")
              .fgGreen()
              .a(leftPad(rowsPerUnitStr, rowsPerUnitLength))
              .reset()
              .a(" | ")
              .fgGreen()
              .a(leftPad(mbPerUnitStr, mbPerUnitLength))
              .reset()
              .a(" | ")
              .fgGreen()
              .a(leftPad(kbPerRowStr, kbPerRowLength));
    }

    // latencies
    if (header.toString().length() < LINE_LENGTH) {
      Snapshot latencies = timer.getSnapshot();
      double p50 = convertDuration(latencies.getMean());
      double p99 = convertDuration(latencies.get99thPercentile());
      double p999 = convertDuration(latencies.get999thPercentile());
      String p50Str = format("%,.2f", p50);
      String p99Str = format("%,.2f", p99);
      String p999Str = format("%,.2f", p999);
      String p50Label = "p50 " + durationUnit;
      String p99Label = "p99" + durationUnit;
      String p999Label = "p999" + durationUnit;
      int p50Length = max(p50Label.length(), p50Str.length());
      int p99Length = max(p99Label.length(), p99Str.length());
      int p999Length = max(p999Label.length(), p999Str.length());
      header =
          header
              .a(" | ")
              .a(leftPad(p50Label, p50Length))
              .a(" | ")
              .a(leftPad(p99Label, p99Length))
              .a(" | ")
              .a(leftPad(p999Label, p999Length));
      message =
          message
              .reset()
              .a(" | ")
              .fgYellow()
              .a(leftPad(p50Str, p50Length))
              .reset()
              .a(" | ")
              .fgYellow()
              .a(leftPad(p99Str, p99Length))
              .reset()
              .a(" | ")
              .fgYellow()
              .a(leftPad(p999Str, p999Length));
    }

    // batches
    if (header.toString().length() < LINE_LENGTH && batchSizes != null) {
      Snapshot snapshot = batchSizes.getSnapshot();
      double avgBatch = snapshot.getMean();
      String avgBatchStr = format("%,.2f", avgBatch);
      int avgBatchLength = max("batches".length(), avgBatchStr.length());
      header = header.a(" | ").a(leftPad("batches", avgBatchLength));
      message = message.reset().a(" | ").fgMagenta().a(leftPad(avgBatchStr, avgBatchLength));
    }

    header = header.reset().newline();
    message = message.reset().newline();

    // print message
    synchronized (stderr) {
      if (!stderr.stale) {
        // If nobody used stderr in the meanwhile, move cursor up two lines,
        // erase these lines and re-print the message.
        System.err.print(
            ansi()
                .cursorUp(1) // ok on  Windows
                .eraseLine(Ansi.Erase.FORWARD) // ok on  Windows
                .cursorUp(1)
                .eraseLine(Ansi.Erase.FORWARD));
      }
      System.err.print(header);
      System.err.print(message);
      stderr.stale = false;
    }
  }

  private static String getAbbreviatedUnit(TimeUnit rateUnit) {
    switch (rateUnit) {
      case NANOSECONDS:
        return "ns";
      case MICROSECONDS:
        return "μs";
      case MILLISECONDS:
        return "ms";
      case SECONDS:
        return "s";
      case MINUTES:
        return "m";
      case HOURS:
        return "h";
      case DAYS:
        return "d";
      default:
        throw new IllegalArgumentException();
    }
  }

  private static class InterceptingPrintStream extends PrintStream {

    // Guarded by this
    private boolean stale = true;

    private InterceptingPrintStream(OutputStream out) {
      super(out);
    }

    @Override
    public void println(String x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void print(boolean b) {
      synchronized (this) {
        super.print(b);
        stale = true;
      }
    }

    @Override
    public void print(char c) {
      synchronized (this) {
        super.print(c);
        stale = true;
      }
    }

    @Override
    public void print(int i) {
      synchronized (this) {
        super.print(i);
        stale = true;
      }
    }

    @Override
    public void print(long l) {
      synchronized (this) {
        super.print(l);
        stale = true;
      }
    }

    @Override
    public void print(float f) {
      synchronized (this) {
        super.print(f);
        stale = true;
      }
    }

    @Override
    public void print(double d) {
      synchronized (this) {
        super.print(d);
        stale = true;
      }
    }

    @Override
    public void print(@NotNull char[] s) {
      synchronized (this) {
        super.print(s);
        stale = true;
      }
    }

    @Override
    public void print(String s) {
      synchronized (this) {
        super.print(s);
        stale = true;
      }
    }

    @Override
    public void print(Object obj) {
      synchronized (this) {
        super.print(obj);
        stale = true;
      }
    }

    @Override
    public void println() {
      synchronized (this) {
        super.println();
        stale = true;
      }
    }

    @Override
    public void println(boolean x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(char x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(int x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(long x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(float x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(double x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(@NotNull char[] x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public void println(Object x) {
      synchronized (this) {
        super.println(x);
        stale = true;
      }
    }

    @Override
    public PrintStream printf(@NotNull String format, Object... args) {
      synchronized (this) {
        super.printf(format, args);
        stale = true;
        return this;
      }
    }

    @Override
    public PrintStream printf(Locale l, @NotNull String format, Object... args) {
      synchronized (this) {
        super.printf(l, format, args);
        stale = true;
        return this;
      }
    }

    @Override
    public PrintStream format(@NotNull String format, Object... args) {
      synchronized (this) {
        super.format(format, args);
        stale = true;
        return this;
      }
    }

    @Override
    public PrintStream format(Locale l, @NotNull String format, Object... args) {
      synchronized (this) {
        super.format(l, format, args);
        stale = true;
        return this;
      }
    }

    @Override
    public void write(int b) {
      synchronized (this) {
        super.write(b);
        stale = true;
      }
    }

    @Override
    public void write(@NotNull byte[] buf, int off, int len) {
      synchronized (this) {
        super.write(buf, off, len);
        stale = true;
      }
    }
  }
}
