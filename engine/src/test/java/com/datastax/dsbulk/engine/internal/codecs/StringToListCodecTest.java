/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static com.datastax.driver.core.TypeCodec.cdouble;
import static com.datastax.driver.core.TypeCodec.list;
import static com.datastax.driver.core.TypeCodec.varchar;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import com.google.common.collect.Lists;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

public class StringToListCodecTest {

  private StringToDoubleCodec eltCodec1 =
      new StringToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  private ExtendedCodecRegistry.StringToStringCodec eltCodec2 =
      new ExtendedCodecRegistry.StringToStringCodec(TypeCodec.varchar());

  private StringToListCodec<Double> codec1 =
      new StringToListCodec<>(list(cdouble()), eltCodec1, "|");
  private StringToListCodec<String> codec2 =
      new StringToListCodec<>(list(varchar()), eltCodec2, "|");

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec1)
        .convertsFrom("1|2|3")
        .to(Lists.newArrayList(1d, 2d, 3d))
        .convertsFrom("1 | 2 | 3")
        .to(Lists.newArrayList(1d, 2d, 3d))
        .convertsFrom("1,234.56|78,900")
        .to(Lists.newArrayList(1234.56d, 78900d))
        .convertsFrom("|")
        .to(Lists.newArrayList(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
    assertThat(codec2)
        .convertsFrom("foo|bar")
        .to(Lists.newArrayList("foo", "bar"))
        .convertsFrom(" foo | bar ")
        .to(Lists.newArrayList("foo", "bar"))
        .convertsFrom("|")
        .to(Lists.newArrayList(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec1)
        .convertsTo(Lists.newArrayList(1d, 2d, 3d))
        .from("1|2|3")
        .convertsTo(Lists.newArrayList(1234.56d, 78900d))
        .from("1,234.56|78,900")
        .convertsTo(Lists.newArrayList(1d, null))
        .from("1|")
        .convertsTo(Lists.newArrayList(null, 0d))
        .from("|0")
        .convertsTo(Lists.newArrayList(null, null))
        .from("|")
        .convertsTo(null)
        .from(null);
    assertThat(codec2)
        .convertsTo(Lists.newArrayList("foo", "bar"))
        .from("foo|bar")
        .convertsTo(Lists.newArrayList(null, null))
        .from("|")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec1).cannotConvertFrom("1|not a valid double");
  }
}
