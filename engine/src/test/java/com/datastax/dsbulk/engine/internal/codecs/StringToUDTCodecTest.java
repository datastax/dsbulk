/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreTestHooks.newField;
import static com.datastax.driver.core.DriverCoreTestHooks.newUserType;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import com.google.common.collect.ImmutableMap;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class StringToUDTCodecTest {

  private UserType udtType;
  private StringToUDTCodec codec;

  @Before
  public void setUp() throws Exception {
    udtType = newUserType(newField("f1", cint()), newField("f2", varchar()));
    ConvertingCodec codec1 =
        new StringToIntegerCodec(
            ThreadLocal.withInitial(
                () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));
    ConvertingCodec codec2 = new ExtendedCodecRegistry.StringToStringCodec(TypeCodec.varchar());
    //noinspection unchecked
    Map<String, ConvertingCodec<String, Object>> fieldCodecs =
        ImmutableMap.of("f1", codec1, "f2", codec2);
    codec = new StringToUDTCodec(TypeCodec.userType(udtType), fieldCodecs, ",", ":");
  }

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("f1:42,f2:foo")
        .to(udtType.newValue().setInt("f1", 42).setString("f2", "foo"))
        .convertsFrom(" f2 : foo , f1 : 42 ")
        .to(udtType.newValue().setInt("f1", 42).setString("f2", "foo"))
        .convertsFrom(" f2 :  , f1 :  ")
        .to(udtType.newValue().setToNull("f1").setToNull("f2"))
        .convertsFrom(" f1 :  ")
        .to(udtType.newValue().setToNull("f1").setToNull("f2"))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(udtType.newValue().setInt("f1", 42).setString("f2", "foo"))
        .from("f1:42,f2:foo")
        .convertsTo(udtType.newValue())
        .from("f1:,f2:")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("f1:not a valid input , f2:not a valid input")
        .cannotConvertFrom(":42") // null field name
        .cannotConvertFrom("not a valid input")
        .cannotConvertFrom("not a,valid input");
  }
}
