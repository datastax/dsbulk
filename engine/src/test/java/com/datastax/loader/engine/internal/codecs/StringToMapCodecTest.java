/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import org.junit.Test;

public class StringToMapCodecTest {

  private ConvertingCodec keyCodec =
      new StringToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  private ConvertingCodec valueCodec =
      new ExtendedCodecRegistry.StringToStringCodec(TypeCodec.varchar());

  @SuppressWarnings("unchecked")
  private StringToMapCodec<Double, String> codec =
      new StringToMapCodec<Double, String>(
          TypeCodec.map(TypeCodec.cdouble(), TypeCodec.varchar()), keyCodec, valueCodec, ";", ":");

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("1:foo;2:bar")
        .to(newLinkedHashMap(1d, "foo", 2d, "bar"))
        .convertsFrom("1,234.56:foo;0.12:bar")
        .to(newLinkedHashMap(1234.56d, "foo", 0.12d, "bar"))
        .convertsFrom(" 1 : foo ; 2 : bar ")
        .to(newLinkedHashMap(1d, "foo", 2d, "bar"))
        .convertsFrom("1:;:foo")
        .to(newLinkedHashMap(1d, null, null, "foo"))
        .convertsFrom(":foo;1:")
        .to(newLinkedHashMap(null, "foo", 1d, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(newLinkedHashMap(1d, "foo", 2d, "bar"))
        .from("1:foo;2:bar")
        .convertsTo(newLinkedHashMap(1d, null, null, "foo"))
        .from("1:;:foo")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid input").cannotConvertFrom("not a;valid input");
  }

  private static Map<Double, String> newLinkedHashMap(Double k1, String v1, Double k2, String v2) {
    Map<Double, String> map = new LinkedHashMap<>();
    map.put(k1, v1);
    map.put(k2, v2);
    return map;
  }
}
