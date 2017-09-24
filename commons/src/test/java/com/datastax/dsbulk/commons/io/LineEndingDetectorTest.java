/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.io;

import com.google.common.base.Charsets;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import org.assertj.core.api.Assertions;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class LineEndingDetectorTest {

  private static final String CR = "\r";
  private static final String LF = "\n";

  private Charset charset;
  private String eol;

  public LineEndingDetectorTest(
      Charset charset, @SuppressWarnings("unused") String eolDescription, String eol) {
    this.charset = charset;
    this.eol = eol;
  }

  @Parameters(name = "{index}: charset = {0}, eol = {1}")
  public static Collection<Object[]> data() {
    return Arrays.asList(
        new Object[][] {
          {Charsets.UTF_8, "LF", LF},
          {Charsets.UTF_8, "CRLF", CR + LF},
          {Charsets.UTF_16BE, "LF", LF},
          {Charsets.UTF_16BE, "CRLF", CR + LF},
          {Charsets.UTF_16LE, "LF", LF},
          {Charsets.UTF_16LE, "CRLF", CR + LF},
          {Charset.forName("UTF-32"), "LF", LF},
          {Charset.forName("UTF-32"), "CRLF", CR + LF},
          {Charset.forName("Shift-JIS"), "LF", LF},
          {Charset.forName("Shift-JIS"), "CRLF", CR + LF},
          {Charsets.ISO_8859_1, "LF", LF},
          {Charsets.ISO_8859_1, "CRLF", CR + LF},
          {Charsets.US_ASCII, "LF", LF},
          {Charsets.US_ASCII, "CRLF", CR + LF}
        });
  }

  @Test
  public void should_detect_start_of_line() throws Exception {
    LineEndingDetector detector;
    {
      detector = new LineEndingDetector(is(""), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is(eol), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length(eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is(eol + eol), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length(eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length(eol + eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is("line1" + eol), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length("line1" + eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is("line1" + eol + eol), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length("line1" + eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length("line1" + eol + eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is("line1"), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is("line1" + eol + "line2" + eol), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length("line1" + eol));
      Assertions.assertThat(detector.nextStartOfLine())
          .isEqualTo(length("line1" + eol + "line2" + eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
    {
      detector = new LineEndingDetector(is("line1" + eol + "line2"), charset);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(length("line1" + eol));
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
      Assertions.assertThat(detector.nextStartOfLine()).isEqualTo(-1);
    }
  }

  private int length(String text) {
    return text.getBytes(charset).length;
  }

  private InputStream is(String text) {
    return new ByteArrayInputStream(text.getBytes(charset));
  }
}
