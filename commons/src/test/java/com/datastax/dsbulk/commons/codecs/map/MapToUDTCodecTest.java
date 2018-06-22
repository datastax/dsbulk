/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.map;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import org.junit.jupiter.api.Test;

@SuppressWarnings({"ConstantConditions", "unchecked"})
class MapToUDTCodecTest {

  private final UserDefinedType udt1 =
      new UserDefinedTypeBuilder("ks", "udt")
          .withField("f1a", DataTypes.INT)
          .withField("f1b", DataTypes.DOUBLE)
          .build();

  private final UdtValue udt1Empty = udt1.newValue().setToNull("f1a").setToNull("f1b");

  private final UdtValue udt1Value = udt1.newValue().setInt("f1a", 42).setDouble("f1b", 0.12d);

  private final MapToUDTCodec<String, String> udtCodec1 =
      (MapToUDTCodec<String, String>)
          new ExtendedCodecRegistryBuilder()
              .withNullStrings("NULL", "")
              .build()
              .codecFor(udt1, GenericType.mapOf(GenericType.STRING, GenericType.STRING));

  private final ImmutableMap<String, String> stringMap =
      ImmutableMap.<String, String>builder().put("f1a", "42").put("f1b", "0.12").build();

  @Test
  void should_convert_from_valid_external() {
    assertThat(udtCodec1)
        .convertsFromExternal(stringMap)
        .toInternal(udt1Value)
        .convertsFromExternal(
            ImmutableMap.<String, String>builder().put("f1a", "").put("f1b", "NULL").build())
        .toInternal(udt1Empty)
        .convertsFromExternal(null)
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(udtCodec1)
        .convertsFromInternal(udt1Value)
        .toExternal(stringMap)
        .convertsFromInternal(udt1.newValue())
        .toExternal(
            ImmutableMap.<String, String>builder().put("f1a", "NULL").put("f1b", "NULL").build())
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(udtCodec1)
        .cannotConvertFromExternal(ImmutableMap.<String, String>builder().put("f1a", "").build())
        .cannotConvertFromExternal(
            ImmutableMap.<String, String>builder().put("f1a", "").put("f1c", "").build());
  }
}
